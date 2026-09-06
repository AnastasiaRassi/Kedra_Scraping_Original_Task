from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Protocol

from dotenv import load_dotenv

from benchmarks.reproducibility import capture_reproducibility_metadata
from kedra_scraper.source_registry import (
    DEFAULT_SOURCE_REGISTRY_PATH,
    load_source_registry,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

TERMINAL_STATUSES = {"SUCCESS", "FAILURE", "CANCELED"}
ACTIVE_STATUSES = {"STARTING", "STARTED", "CANCELING"}
FAILURE_EVENT_NAMES = {
    "document_request_failed",
    "search_request_failed",
    "item_dropped",
    "persistence_operation_failed",
}


class DagsterClient(Protocol):
    def submit_job_execution(self, job_name: str, **kwargs: Any) -> str: ...

    def get_run_status(self, run_id: str) -> Any: ...


@dataclass
class RunObservation:
    date_partition: str
    partition_key: str
    submitted_at_utc: str
    submitted_elapsed: float
    run_id: str | None = None
    final_status: str = "SUBMITTED"
    started_at_utc: str | None = None
    completed_at_utc: str | None = None
    started_elapsed: float | None = None
    completed_elapsed: float | None = None
    submission_error: str | None = None
    poll_errors: list[str] = field(default_factory=list)
    status_history: list[dict[str, str]] = field(default_factory=list)

    def record_status(self, value: Any, elapsed: float) -> bool:
        status = normalise_status(value)
        if status == self.final_status:
            return False

        now = _utc_now()
        self.final_status = status
        self.status_history.append({"status": status, "observed_at_utc": now})
        if status in ACTIVE_STATUSES and self.started_elapsed is None:
            self.started_elapsed = elapsed
            self.started_at_utc = now
        if status in TERMINAL_STATUSES:
            self.completed_elapsed = elapsed
            self.completed_at_utc = now
        return True

    def to_report(self, source: str, failure_event_limit: int) -> dict[str, Any]:
        paths = artifact_paths(source, self.date_partition, self.run_id)
        crawl_profile = _read_json(paths["crawl_profile_path"])
        scraping_summary = _read_json(paths["scraping_summary_path"])
        log_details = read_structured_log(
            paths["structured_log_path"],
            limit=failure_event_limit,
        )
        queue_seconds = (
            round(self.started_elapsed - self.submitted_elapsed, 3)
            if self.started_elapsed is not None
            else None
        )
        execution_seconds = (
            round(self.completed_elapsed - self.started_elapsed, 3)
            if self.completed_elapsed is not None
            and self.started_elapsed is not None
            else None
        )
        total_seconds = (
            round(self.completed_elapsed - self.submitted_elapsed, 3)
            if self.completed_elapsed is not None
            else None
        )

        return {
            "date_partition": self.date_partition,
            "partition_key": self.partition_key,
            "run_id": self.run_id,
            "final_status": self.final_status,
            "submitted_at_utc": self.submitted_at_utc,
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "queue_seconds": queue_seconds,
            "execution_seconds": execution_seconds,
            "total_seconds": total_seconds,
            "submission_error": self.submission_error,
            "poll_errors": self.poll_errors,
            "status_history": self.status_history,
            **{name: str(path) if path else None for name, path in paths.items()},
            "crawl_profile": crawl_profile,
            "scraping_summary": scraping_summary,
            "failure_events": log_details["failure_events"],
            "failure_event_count": log_details["failure_event_count"],
            "failure_events_truncated": log_details["failure_events_truncated"],
            "raw_asset_attempts_observed": log_details["crawl_summary_events"],
            "raw_asset_retries_observed": max(
                log_details["crawl_summary_events"] - 1,
                0,
            ),
        }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    partitions = month_partition_keys(args.start_partition, args.end_partition)
    if len(partitions) < 2:
        raise SystemExit(
            "Select at least two monthly partitions; this profiler measures "
            "Dagster multi-partition load."
        )
    if len(partitions) > args.max_partitions:
        raise SystemExit(
            f"Selection contains {len(partitions)} partitions, above the safety "
            f"limit of {args.max_partitions}. Pass --max-partitions "
            f"{len(partitions)} explicitly if that load is intentional."
        )

    registry = load_source_registry(args.registry)
    try:
        source_config = registry[args.source]
    except KeyError as exc:
        available = ", ".join(sorted(registry))
        raise SystemExit(
            f"Unknown source {args.source!r}. Available sources: {available}"
        ) from exc

    if os.getenv("PERSISTENCE_ENABLED", "").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        raise SystemExit(
            "Dagster load profiling runs the production persistence path. Set "
            "PERSISTENCE_ENABLED=true and start MongoDB and MinIO first."
        )

    instance_config = resolve_instance_config(args.instance_config)
    configured_concurrency = read_max_concurrent_runs(instance_config)
    if (
        args.expect_max_concurrent_runs is not None
        and configured_concurrency != args.expect_max_concurrent_runs
    ):
        raise SystemExit(
            "The active instance configuration does not match the experiment: "
            f"expected max_concurrent_runs={args.expect_max_concurrent_runs}, "
            f"found {configured_concurrency!r} in {instance_config}."
        )

    config_paths: list[str | Path] = [args.registry, instance_config]
    config_paths.extend(
        value
        for name, value in source_config.spider_settings.items()
        if name.endswith("_CONFIG_PATH")
    )
    reproducibility = capture_reproducibility_metadata(
        project_root=PROJECT_ROOT,
        spider=source_config.spider,
        source=source_config.key,
        spider_args={
            "start_partition": partitions[0],
            "end_partition": partitions[-1],
            "partition_count": str(len(partitions)),
        },
        setting_overrides=dict(source_config.spider_settings),
        config_paths=config_paths,
        with_persistence=True,
        max_items=None,
        log_level=os.getenv("DAGSTER_CRAWL_LOG_LEVEL", "INFO"),
    )
    if args.require_clean_git:
        _require_clean_git(reproducibility)

    destination = args.output or _default_destination(
        source_config.key,
        partitions[0],
        partitions[-1],
    )
    destination = _absolute_path(destination)
    benchmark_id = uuid.uuid4().hex
    started_wall = time.perf_counter()
    started_at = _utc_now()
    observations: list[RunObservation] = []
    peak_active_runs = 0
    profiler_timed_out = False

    print(
        f"Dagster load profile: source={source_config.key} "
        f"partitions={len(partitions)} job={args.job_name}"
    )
    print(
        "Instance concurrency: "
        + (
            str(configured_concurrency)
            if configured_concurrency is not None
            else f"unknown (not found in {instance_config})"
        )
    )
    print(f"Report: {destination}")

    client = create_client(
        args.host,
        args.port,
        use_https=args.https,
        timeout=args.graphql_timeout_seconds,
    )

    for date_partition in partitions:
        elapsed = time.perf_counter() - started_wall
        partition_key = multi_partition_key(date_partition, source_config.key)
        observation = RunObservation(
            date_partition=date_partition,
            partition_key=partition_key,
            submitted_at_utc=_utc_now(),
            submitted_elapsed=elapsed,
        )
        try:
            observation.run_id = client.submit_job_execution(
                args.job_name,
                repository_location_name=args.repository_location,
                repository_name=args.repository,
                tags=partition_tags(
                    date_partition,
                    source_config.key,
                    benchmark_id,
                ),
            )
            observation.status_history.append(
                {
                    "status": "SUBMITTED",
                    "observed_at_utc": observation.submitted_at_utc,
                }
            )
            print(
                f"Submitted {partition_key}: run_id={observation.run_id}"
            )
        except Exception as exc:  # the exact GraphQL exception is versioned
            observation.final_status = "SUBMISSION_FAILED"
            observation.submission_error = f"{type(exc).__name__}: {exc}"
            observation.completed_at_utc = _utc_now()
            observation.completed_elapsed = time.perf_counter() - started_wall
            print(
                f"Submission failed for {partition_key}: "
                f"{observation.submission_error}",
                file=sys.stderr,
            )
        observations.append(observation)

    deadline = started_wall + args.timeout_seconds
    while any(
        observation.run_id
        and observation.final_status not in TERMINAL_STATUSES
        for observation in observations
    ):
        now = time.perf_counter()
        if now >= deadline:
            profiler_timed_out = True
            print(
                "Profiler timeout reached. Dagster runs are left under "
                "Dagster's control and may still be running.",
                file=sys.stderr,
            )
            break

        for observation in observations:
            if (
                not observation.run_id
                or observation.final_status in TERMINAL_STATUSES
            ):
                continue
            try:
                changed = observation.record_status(
                    client.get_run_status(observation.run_id),
                    time.perf_counter() - started_wall,
                )
                if changed:
                    print(
                        f"{observation.partition_key}: "
                        f"{observation.final_status}"
                    )
            except Exception as exc:  # preserve transient control-plane errors
                message = f"{type(exc).__name__}: {exc}"
                if not observation.poll_errors or observation.poll_errors[-1] != message:
                    observation.poll_errors.append(message)

        active = sum(
            observation.final_status in ACTIVE_STATUSES
            for observation in observations
        )
        peak_active_runs = max(peak_active_runs, active)
        if any(
            observation.run_id
            and observation.final_status not in TERMINAL_STATUSES
            for observation in observations
        ):
            time.sleep(args.poll_seconds)

    completed_at = _utc_now()
    wall_duration = time.perf_counter() - started_wall
    run_reports = [
        observation.to_report(source_config.key, args.failure_event_limit)
        for observation in observations
    ]
    aggregate, reliability = aggregate_run_reports(
        run_reports,
        wall_duration_seconds=wall_duration,
        peak_active_runs=peak_active_runs,
        profiler_timed_out=profiler_timed_out,
    )
    reproducibility["completed_at_utc"] = completed_at
    reproducibility["invocation"].update(
        {
            "benchmark_type": "dagster_multi_partition_load",
            "job_name": args.job_name,
            "partitions": partitions,
            "dagster_host": args.host,
            "dagster_port": args.port,
            "dagster_https": args.https,
            "configured_max_concurrent_runs": configured_concurrency,
        }
    )
    report = {
        "schema_version": 1,
        "benchmark_type": "dagster_multi_partition_load",
        "benchmark_id": benchmark_id,
        "generated_at_utc": completed_at,
        "started_at_utc": started_at,
        "source": source_config.key,
        "job_name": args.job_name,
        "dagster_endpoint": {
            "host": args.host,
            "port": args.port,
            "https": args.https,
        },
        "instance_config_path": str(instance_config),
        "configured_max_concurrent_runs": configured_concurrency,
        "requested_partitions": partitions,
        "wall_duration_seconds": round(wall_duration, 3),
        "profiler_timed_out": profiler_timed_out,
        "runs": run_reports,
        "aggregate": aggregate,
        "reliability": reliability,
        "reproducibility": reproducibility,
    }
    _write_report(destination, report)
    print_load_profile(report, destination)
    return 0 if reliability["passed"] else 1


def month_partition_keys(start_value: str, end_value: str) -> list[str]:
    start = _partition_date(start_value)
    end = _partition_date(end_value)
    if end < start:
        raise SystemExit("--end-partition must not precede --start-partition")

    result: list[str] = []
    current = start
    while current <= end:
        result.append(current.isoformat())
        current = date(
            current.year + (1 if current.month == 12 else 0),
            1 if current.month == 12 else current.month + 1,
            1,
        )
    return result


def multi_partition_key(date_partition: str, source: str) -> str:
    """Mirror Dagster's alphabetically ordered MultiPartitionKey string."""
    return f"{date_partition}|{source}"


def partition_tags(
    date_partition: str,
    source: str,
    benchmark_id: str,
) -> dict[str, str]:
    return {
        "dagster/partition": multi_partition_key(date_partition, source),
        "dagster/partition/date": date_partition,
        "dagster/partition/source": source,
        "kedra/benchmark": "true",
        "kedra/benchmark_id": benchmark_id,
    }


def aggregate_run_reports(
    runs: list[dict[str, Any]],
    *,
    wall_duration_seconds: float,
    peak_active_runs: int,
    profiler_timed_out: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    profiles = [
        run["crawl_profile"]
        for run in runs
        if isinstance(run.get("crawl_profile"), dict)
    ]
    scraping_summaries = [
        run["scraping_summary"]
        for run in runs
        if isinstance(run.get("scraping_summary"), dict)
    ]
    body_partitions = [
        {
            "run_id": run.get("run_id"),
            **row,
        }
        for run in runs
        if isinstance(run.get("crawl_profile"), dict)
        for row in run["crawl_profile"].get("body_partitions", [])
        if isinstance(row, dict)
    ]
    status_counts = Counter(run["final_status"] for run in runs)
    http_status_counts: Counter[str] = Counter()
    retry_reasons: Counter[str] = Counter()
    error_counts: Counter[str] = Counter()
    for profile in profiles:
        http_status_counts.update(profile.get("requests", {}).get("http_status_counts", {}))
        retry_reasons.update(profile.get("retries", {}).get("reason_counts", {}))
        error_counts.update(profile.get("failures", {}).get("error_counts", {}))

    expected = _sum_nested(profiles, "documents", "expected")
    ingested = _sum_nested(profiles, "documents", "scraped")
    missing = _sum_nested(profiles, "documents", "missing")
    unexplained = _sum_nested(profiles, "documents", "unexplained_missing")
    newly_scraped = _sum_values(scraping_summaries, "scraped")
    unchanged = _sum_values(scraping_summaries, "unchanged")
    scraping_failed = _sum_values(scraping_summaries, "scraping_failed")
    completed_documents = newly_scraped + unchanged
    duration = max(float(wall_duration_seconds), 0.0)

    aggregate = {
        "runs": {
            "requested": len(runs),
            "status_counts": dict(sorted(status_counts.items())),
            "successful": status_counts["SUCCESS"],
            "failed": status_counts["FAILURE"],
            "canceled": status_counts["CANCELED"],
            "submission_failed": status_counts["SUBMISSION_FAILED"],
            "nonterminal_at_report": sum(
                status not in TERMINAL_STATUSES
                and status != "SUBMISSION_FAILED"
                for status in (run["final_status"] for run in runs)
            ),
            "crawl_profiles_missing": len(runs) - len(profiles),
            "scraping_summaries_missing": len(runs) - len(scraping_summaries),
            "observed_peak_active": peak_active_runs,
            "queue_seconds": numeric_summary(
                run.get("queue_seconds") for run in runs
            ),
            "execution_seconds": numeric_summary(
                run.get("execution_seconds") for run in runs
            ),
            "total_seconds": numeric_summary(
                run.get("total_seconds") for run in runs
            ),
        },
        "documents": {
            "expected": expected,
            "raw_ingested": ingested,
            "raw_missing": missing,
            "raw_unexplained_missing": unexplained,
            "raw_documents_processed_by_scraper": _sum_values(
                scraping_summaries,
                "raw_documents",
            ),
            "newly_scraped": newly_scraped,
            "unchanged": unchanged,
            "scraping_failed": scraping_failed,
            "pipeline_completed": completed_documents,
            "raw_ingestion_throughput_per_minute": _throughput(
                ingested,
                duration,
            ),
            "pipeline_throughput_per_minute": _throughput(
                completed_documents,
                duration,
            ),
        },
        "requests": {
            "sent": _sum_nested(profiles, "requests", "sent"),
            "responses": _sum_nested(profiles, "requests", "responses"),
            "response_bytes": _sum_nested(
                profiles,
                "requests",
                "response_bytes",
            ),
            "http_status_counts": dict(sorted(http_status_counts.items())),
        },
        "latency_seconds": aggregate_latency(profiles),
        "retries": {
            "scrapy_attempts": _sum_nested(profiles, "retries", "attempts"),
            "scrapy_recovered": _sum_nested(
                profiles,
                "retries",
                "successful_responses_after_retry",
            ),
            "scrapy_exhausted": _sum_nested(
                profiles,
                "retries",
                "exhausted",
            ),
            "scrapy_reason_counts": dict(sorted(retry_reasons.items())),
            "raw_asset_retries_observed": sum(
                _integer(run.get("raw_asset_retries_observed")) for run in runs
            ),
        },
        "failures": {
            "dagster_failed_runs": status_counts["FAILURE"],
            "submission_failures": status_counts["SUBMISSION_FAILED"],
            "document_requests": _sum_nested(
                profiles,
                "failures",
                "document_requests",
            ),
            "search_requests": _sum_nested(
                profiles,
                "failures",
                "search_requests",
            ),
            "raw_extraction": _sum_nested(
                profiles,
                "failures",
                "extraction",
            ),
            "incomplete_results": _sum_nested(
                profiles,
                "failures",
                "incomplete_results",
            ),
            "dropped_items": _sum_nested(
                profiles,
                "failures",
                "dropped_items",
            ),
            "persistence_errors": _sum_nested(
                profiles,
                "persistence",
                "errors",
            ),
            "scraping_failed": scraping_failed,
            "structured_failure_events": sum(
                _integer(run.get("failure_event_count")) for run in runs
            ),
            "poll_errors": sum(len(run.get("poll_errors") or []) for run in runs),
            "error_counts": dict(sorted(error_counts.items())),
        },
        "persistence": aggregate_persistence(profiles),
        "body_partitions": body_partitions,
        "wall_duration_seconds": round(duration, 3),
    }

    violations: list[str] = []
    if profiler_timed_out:
        violations.append("the profiler timed out before every run finished")
    if status_counts["SUCCESS"] != len(runs):
        violations.append(
            f"only {status_counts['SUCCESS']} of {len(runs)} Dagster runs succeeded"
        )
    if len(profiles) != len(runs):
        violations.append(
            f"{len(runs) - len(profiles)} run(s) have no crawl profile"
        )
    if len(scraping_summaries) != len(runs):
        violations.append(
            f"{len(runs) - len(scraping_summaries)} run(s) have no scraping summary"
        )
    strict_failure_checks = {
        "final document request failures": aggregate["failures"]["document_requests"],
        "final search request failures": aggregate["failures"]["search_requests"],
        "unexplained missing raw documents": unexplained,
        "persistence errors": aggregate["failures"]["persistence_errors"],
        "document scraping failures": scraping_failed,
    }
    violations.extend(
        f"{label}={count}"
        for label, count in strict_failure_checks.items()
        if count
    )
    reliability = {
        "passed": not violations,
        "violations": violations,
        "successful_partition_rate": round(
            status_counts["SUCCESS"] / len(runs),
            6,
        )
        if runs
        else 0.0,
    }
    return aggregate, reliability


def aggregate_latency(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [profile.get("latency_seconds", {}) for profile in profiles]
    rows = [row for row in rows if _integer(row.get("samples")) > 0]
    samples = sum(_integer(row.get("samples")) for row in rows)
    weighted_mean = (
        sum(
            float(row["mean"]) * _integer(row.get("samples"))
            for row in rows
            if isinstance(row.get("mean"), (int, float))
        )
        / samples
        if samples
        else None
    )
    return {
        "samples": samples,
        "minimum": _minimum(row.get("minimum") for row in rows),
        "weighted_mean": round(weighted_mean, 4) if weighted_mean is not None else None,
        "partition_p50_mean": _average(row.get("p50") for row in rows),
        "partition_p95_max": _maximum(row.get("p95") for row in rows),
        "partition_p99_max": _maximum(row.get("p99") for row in rows),
        "maximum": _maximum(row.get("maximum") for row in rows),
        "note": (
            "Percentiles are not pooled across partitions; the report records "
            "the mean partition p50 and worst partition p95/p99."
        ),
    }


def aggregate_persistence(profiles: list[dict[str, Any]]) -> dict[str, int]:
    names = (
        "errors",
        "retry_attempts",
        "retry_recovered",
        "retry_exhausted",
        "minio_uploaded",
        "minio_unchanged",
        "mongodb_inserted",
        "mongodb_updated",
        "mongodb_unchanged",
    )
    return {
        name: _sum_nested(profiles, "persistence", name) for name in names
    }


def numeric_summary(values: Any) -> dict[str, int | float | None]:
    samples = sorted(
        float(value)
        for value in values
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    )
    if not samples:
        return {
            "samples": 0,
            "minimum": None,
            "mean": None,
            "p50": None,
            "p95": None,
            "maximum": None,
        }
    return {
        "samples": len(samples),
        "minimum": round(samples[0], 3),
        "mean": round(mean(samples), 3),
        "p50": round(_percentile(samples, 50), 3),
        "p95": round(_percentile(samples, 95), 3),
        "maximum": round(samples[-1], 3),
    }


def artifact_paths(
    source: str,
    partition_date: str,
    run_id: str | None,
) -> dict[str, Path | None]:
    if not run_id:
        return {
            "structured_log_path": None,
            "crawl_profile_path": None,
            "scraping_summary_path": None,
        }
    root = Path(os.getenv("SCRAPY_LOG_DIR", "logs")).expanduser()
    if not root.is_absolute():
        root = PROJECT_ROOT / root
    safe_source = _safe_component(source, "source")
    safe_run_id = _safe_component(run_id, "run")
    directory = root / "dagster" / safe_source / partition_date
    structured_log = directory / f"raw_documents_{safe_run_id}.jsonl"
    return {
        "structured_log_path": structured_log,
        "crawl_profile_path": structured_log.with_suffix(".profile.json"),
        "scraping_summary_path": (
            directory / f"scraped_documents_{safe_run_id}.summary.json"
        ),
    }


def read_structured_log(path: Path | None, *, limit: int) -> dict[str, Any]:
    failure_events: list[dict[str, Any]] = []
    failure_count = 0
    crawl_summary_events = 0
    if path:
        candidates = sorted(path.parent.glob(f"{path.name}*"))
        for candidate in candidates:
            try:
                lines = candidate.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeError):
                continue
            for line in lines:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, dict):
                    continue
                if payload.get("event") == "crawl_summary":
                    crawl_summary_events += 1
                if not _is_failure_event(payload):
                    continue
                failure_count += 1
                if len(failure_events) < limit:
                    failure_events.append(_compact_failure(payload))
    return {
        "failure_events": failure_events,
        "failure_event_count": failure_count,
        "failure_events_truncated": max(failure_count - len(failure_events), 0),
        "crawl_summary_events": crawl_summary_events,
    }


def read_max_concurrent_runs(path: Path) -> int | None:
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None
    match = re.search(r"(?m)^\s*max_concurrent_runs:\s*(\d+)\s*(?:#.*)?$", content)
    return int(match.group(1)) if match else None


def resolve_instance_config(value: Path | None) -> Path:
    if value:
        return _absolute_path(value)
    dagster_home = os.getenv("DAGSTER_HOME")
    if dagster_home:
        return Path(dagster_home).expanduser().resolve() / "dagster.yaml"
    return PROJECT_ROOT / ".dagster" / "dagster.yaml"


def normalise_status(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw).rsplit(".", 1)[-1].upper()


def create_client(
    host: str,
    port: int,
    *,
    use_https: bool,
    timeout: int,
) -> DagsterClient:
    try:
        from dagster_graphql import DagsterGraphQLClient
    except ImportError as exc:
        raise SystemExit(
            "dagster-graphql is required. Install the updated requirements.txt."
        ) from exc
    try:
        return DagsterGraphQLClient(
            host,
            port_number=port,
            use_https=use_https,
            timeout=timeout,
        )
    except Exception as exc:
        raise SystemExit(f"Could not connect to Dagster: {exc}") from exc


def print_load_profile(report: dict[str, Any], destination: Path) -> None:
    aggregate = report["aggregate"]
    runs = aggregate["runs"]
    documents = aggregate["documents"]
    failures = aggregate["failures"]
    retries = aggregate["retries"]
    latency = aggregate["latency_seconds"]

    print("\nDagster multi-partition load profile")
    print(f"  Report: {destination}")
    print(
        f"  Runs: success={runs['successful']}/{runs['requested']} "
        f"peak_active={runs['observed_peak_active']} "
        f"statuses={runs['status_counts']}"
    )
    print(
        f"  Wall time: {aggregate['wall_duration_seconds']:.3f}s; "
        f"pipeline throughput: "
        f"{documents['pipeline_throughput_per_minute']:.3f} records/min"
    )
    print(
        "  Documents: "
        f"expected={documents['expected']} ingested={documents['raw_ingested']} "
        f"newly_scraped={documents['newly_scraped']} "
        f"unchanged={documents['unchanged']} "
        f"failed={documents['scraping_failed']}"
    )
    print(
        "  Latency: "
        f"weighted_mean={_display_seconds(latency['weighted_mean'])} "
        f"worst_partition_p95={_display_seconds(latency['partition_p95_max'])}"
    )
    print(
        "  Retries: "
        f"scrapy={retries['scrapy_attempts']} "
        f"recovered={retries['scrapy_recovered']} "
        f"exhausted={retries['scrapy_exhausted']} "
        f"dagster_raw_asset={retries['raw_asset_retries_observed']}"
    )
    print(
        "  Failures: "
        f"runs={failures['dagster_failed_runs']} "
        f"requests={failures['document_requests']} "
        f"search={failures['search_requests']} "
        f"persistence={failures['persistence_errors']} "
        f"scraping={failures['scraping_failed']}"
    )
    print(
        "  Reliability: "
        + (
            "PASS"
            if report["reliability"]["passed"]
            else "FAIL - " + "; ".join(report["reliability"]["violations"])
        )
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Submit a source across multiple monthly partitions to the real "
            "Dagster job and write one load/reliability report."
        )
    )
    parser.add_argument("--source", required=True, help="Source key in the registry.")
    parser.add_argument(
        "--start-partition",
        required=True,
        help="First monthly partition key (YYYY-MM-01).",
    )
    parser.add_argument(
        "--end-partition",
        required=True,
        help="Last monthly partition key, inclusive (YYYY-MM-01).",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=_positive_integer, default=3000)
    parser.add_argument("--https", action="store_true")
    parser.add_argument("--job-name", default="document_pipeline_job")
    parser.add_argument("--repository-location")
    parser.add_argument("--repository")
    parser.add_argument(
        "--registry",
        default=os.getenv("SOURCE_REGISTRY_PATH", DEFAULT_SOURCE_REGISTRY_PATH),
    )
    parser.add_argument("--instance-config", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--poll-seconds",
        type=_positive_float,
        default=2.0,
    )
    parser.add_argument(
        "--timeout-seconds",
        type=_positive_float,
        default=float(os.getenv("DAGSTER_BENCHMARK_TIMEOUT_SECONDS", "14400")),
    )
    parser.add_argument(
        "--graphql-timeout-seconds",
        type=_positive_integer,
        default=60,
    )
    parser.add_argument(
        "--max-partitions",
        type=_positive_integer,
        default=12,
        help="Safety cap; increase explicitly for a larger test.",
    )
    parser.add_argument(
        "--expect-max-concurrent-runs",
        type=_positive_integer,
        help="Abort if the active dagster.yaml does not contain this limit.",
    )
    parser.add_argument(
        "--failure-event-limit",
        type=_positive_integer,
        default=200,
        help="Maximum exact failure events retained per partition.",
    )
    parser.add_argument(
        "--require-clean-git",
        action="store_true",
        help="Refuse to benchmark uncommitted code.",
    )
    return parser


def _partition_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid partition {value!r}; use YYYY-MM-01") from exc
    if parsed.day != 1:
        raise SystemExit(f"Invalid partition {value!r}; the day must be 01")
    return parsed


def _require_clean_git(reproducibility: dict[str, Any]) -> None:
    git = reproducibility["git"]
    if not git["available"]:
        raise SystemExit(
            "--require-clean-git was requested, but Git state could not be inspected."
        )
    if git["dirty"]:
        raise SystemExit(
            "--require-clean-git was requested, but the working tree has local changes."
        )


def _default_destination(source: str, start: str, end: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    safe_source = _safe_component(source, "source")
    return PROJECT_ROOT / "reports" / "dagster_load_profiles" / (
        f"{safe_source}_{start}_{end}_{timestamp}.json"
    )


def _absolute_path(path: Path) -> Path:
    path = path.expanduser()
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if not path or not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _is_failure_event(payload: dict[str, Any]) -> bool:
    event = str(payload.get("event") or "")
    return (
        payload.get("level") == "ERROR"
        or event in FAILURE_EVENT_NAMES
        or event.endswith("_failed")
    )


def _compact_failure(payload: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "timestamp",
        "level",
        "logger",
        "event",
        "message",
        "partition_date",
        "body",
        "identifier",
        "url",
        "status_code",
        "error_type",
        "reason",
    )
    result = {name: payload[name] for name in fields if name in payload}
    for name in ("message", "reason"):
        if isinstance(result.get(name), str):
            result[name] = result[name][:4000]
    return result


def _sum_nested(rows: list[dict[str, Any]], section: str, name: str) -> int:
    return sum(_integer(row.get(section, {}).get(name)) for row in rows)


def _sum_values(rows: list[dict[str, Any]], name: str) -> int:
    return sum(_integer(row.get(name)) for row in rows)


def _integer(value: Any) -> int:
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0


def _numeric(values: Any) -> list[float]:
    return [
        float(value)
        for value in values
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    ]


def _minimum(values: Any) -> float | None:
    samples = _numeric(values)
    return round(min(samples), 4) if samples else None


def _maximum(values: Any) -> float | None:
    samples = _numeric(values)
    return round(max(samples), 4) if samples else None


def _average(values: Any) -> float | None:
    samples = _numeric(values)
    return round(mean(samples), 4) if samples else None


def _percentile(sorted_values: list[float], percentile: float) -> float:
    position = (len(sorted_values) - 1) * percentile / 100.0
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction


def _throughput(count: int, duration: float) -> float:
    return round(count * 60.0 / duration, 3) if duration else 0.0


def _safe_component(value: str, fallback: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
    return safe or fallback


def _display_seconds(value: Any) -> str:
    return f"{value:.4f}s" if isinstance(value, (int, float)) else "n/a"


def _positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
