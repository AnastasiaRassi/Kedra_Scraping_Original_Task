import json
import os
import subprocess
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any
SOURCE_REGISTRY = load_source_registry(
    os.getenv("SOURCE_REGISTRY_PATH", "config/sources.json")
)
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Backoff,
    Definitions,
    Failure,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    RetryPolicy,
    RunRequest,
    SkipReason,
    StaticPartitionsDefinition,
    asset,
    define_asset_job,
    schedule,
)
from dotenv import load_dotenv

from kedra_scraper.config import SourceRegistryEntry, load_source_registry
from kedra_scraper.scraping import scrape_partition
from kedra_scraper.utils import env_bool, env_csv, env_float, env_int


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(
    start_date=os.getenv("DAGSTER_PARTITION_START_DATE", "2000-01-01"),
    end_date=os.getenv("DAGSTER_PARTITION_END_DATE") or None,
    timezone=os.getenv("DAGSTER_PARTITION_TIMEZONE", "Europe/Dublin"),
    end_offset=env_int("DAGSTER_PARTITION_END_OFFSET", 0),
)

SOURCE_MONTH_PARTITIONS = MultiPartitionsDefinition(
    {
        "date": MONTHLY_PARTITIONS,
        "source": StaticPartitionsDefinition(sorted(SOURCE_REGISTRY)),
    }
)

TASK_RETRY_POLICY = RetryPolicy(
    max_retries=env_int("DAGSTER_CRAWL_MAX_RETRIES", 2, minimum=0),
    delay=env_float(
        "DAGSTER_CRAWL_RETRY_DELAY_SECONDS",
        30.0,
        minimum=0.0,
    ),
    backoff=Backoff.EXPONENTIAL,
)


@asset(
    name="raw_documents",
    group_name="document_ingestion",
    description=(
        "Ingest original source documents and metadata into MinIO and MongoDB."
    ),
    partitions_def=SOURCE_MONTH_PARTITIONS,
    retry_policy=TASK_RETRY_POLICY,
)
def raw_documents(context: AssetExecutionContext) -> MaterializeResult:
    """Run one configured source spider for one calendar month."""
    _require_persistence()
    source_config, partition_date, start_date, end_date = _partition_context(
        context
    )
    timeout_seconds = env_float(
        "DAGSTER_CRAWL_TIMEOUT_SECONDS",
        7200.0,
        minimum=1.0,
    )
    log_level = os.getenv("DAGSTER_CRAWL_LOG_LEVEL", "INFO").strip().upper()

    with tempfile.TemporaryDirectory(prefix="kedra-ingestion-") as directory:
        summary_path = Path(directory) / "crawl-summary.json"
        command = [
            sys.executable,
            "-m",
            "scrapy",
            "crawl",
            source_config.spider,
            "-a",
            f"start_date={start_date:%d-%m-%Y}",
            "-a",
            f"end_date={end_date:%d-%m-%Y}",
            "-s",
            "SCRAPE_MODE=ingestion",
            "-s",
            f"CRAWL_SUMMARY_PATH={summary_path}",
            "-s",
            f"LOG_LEVEL={log_level}",
        ]
        for name, value in source_config.spider_settings.items():
            command.extend(("-s", f"{name}={value}"))

        context.log.info(
            "Starting source=%s ingestion for %s through %s",
            source_config.key,
            start_date.isoformat(),
            end_date.isoformat(),
        )
        result = _run_scrapy(
            context,
            command,
            timeout_seconds=timeout_seconds,
        )
        summary = _read_summary(summary_path)
        violations = _ingestion_violations(summary)

        if violations:
            _log_subprocess_tail(context, "stderr", result.stderr)
            raise Failure(
                description="; ".join(violations),
                metadata=summary,
                allow_retries=True,
            )

        context.log.info(
            "Raw ingestion completed: expected=%s stored=%s",
            summary.get("expected", 0),
            summary.get("scraped", 0),
        )
        return MaterializeResult(
            metadata={
                "source": source_config.key,
                "partition_start": start_date.isoformat(),
                "partition_end": end_date.isoformat(),
                **summary,
            }
        )


@asset(
    name="scraped_documents",
    group_name="document_scraping",
    description=(
        "Extract and normalize text from raw MinIO documents into MongoDB."
    ),
    deps=[raw_documents],
    partitions_def=SOURCE_MONTH_PARTITIONS,
    retry_policy=TASK_RETRY_POLICY,
)
def scraped_documents(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape the same source/month only after raw ingestion succeeds."""
    _require_persistence()
    source_config, partition_date, start_date, end_date = _partition_context(
        context
    )

    context.log.info(
        "Starting source=%s scraping for partition=%s",
        source_config.key,
        partition_date,
    )
    try:
        summary = scrape_partition(source_config, partition_date)
    except Exception as exc:
        raise Failure(
            description=(
                f"Scraping failed operationally for source="
                f"{source_config.key}, partition={partition_date}: {exc}"
            ),
            allow_retries=True,
        ) from exc

    failed = summary["scraping_failed"]
    if failed:
        context.log.warning(
            "Scraping completed with %s document extraction failure(s)",
            failed,
        )
        if env_bool("DAGSTER_FAIL_ON_SCRAPING_ERRORS", False):
            raise Failure(
                description=(
                    f"{failed} document(s) could not be scraped for "
                    f"source={source_config.key}, partition={partition_date}"
                ),
                metadata=summary,
                allow_retries=False,
            )

    context.log.info(
        "Scraping completed: scraped=%s unchanged=%s failed=%s",
        summary["scraped"],
        summary["unchanged"],
        failed,
    )
    return MaterializeResult(
        metadata={
            "source": source_config.key,
            "partition_start": start_date.isoformat(),
            "partition_end": end_date.isoformat(),
            **summary,
        }
    )


def _partition_context(
    context: AssetExecutionContext,
) -> tuple[SourceRegistryEntry, str, date, date]:
    partition_key = context.partition_key
    if not isinstance(partition_key, MultiPartitionKey):
        raise Failure(
            description="Expected a source/date multi-partition key",
            allow_retries=False,
        )

    dimensions = partition_key.keys_by_dimension
    source_key = dimensions["source"]
    date_key = dimensions["date"]
    source_config = SOURCE_REGISTRY.get(source_key)
    if source_config is None:
        raise Failure(
            description=f"Unknown configured source: {source_key}",
            allow_retries=False,
        )

    window = MONTHLY_PARTITIONS.time_window_for_partition_key(date_key)
    start_date = window.start.date()
    end_date = (window.end - timedelta(days=1)).date()
    return source_config, start_date.isoformat(), start_date, end_date


def _run_scrapy(
    context: AssetExecutionContext,
    command: list[str],
    *,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        _log_subprocess_tail(context, "stdout", exc.stdout)
        _log_subprocess_tail(context, "stderr", exc.stderr)
        raise Failure(
            description=f"Scrapy exceeded {timeout_seconds:g} seconds",
            allow_retries=True,
        ) from exc
    except OSError as exc:
        raise Failure(
            description=f"Could not start the Scrapy process: {exc}",
            allow_retries=True,
        ) from exc

    if result.returncode != 0:
        stdout_tail = _subprocess_tail(result.stdout)
        stderr_tail = _subprocess_tail(result.stderr)
        _log_subprocess_tail(context, "stdout", stdout_tail)
        _log_subprocess_tail(context, "stderr", stderr_tail)

        diagnostic = stderr_tail or stdout_tail or "Scrapy produced no output."
        raise Failure(
            description=(
                f"Scrapy exited with code {result.returncode}. "
                f"Last subprocess output:\n{diagnostic}"
            ),
            metadata={"return_code": result.returncode},
            allow_retries=True,
        )
    return result


def _read_summary(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise Failure(
            description="Scrapy finished without producing a crawl summary",
            allow_retries=True,
        )

    try:
        summary = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Failure(
            description=f"Could not read the Scrapy crawl summary: {exc}",
            allow_retries=True,
        ) from exc

    if not isinstance(summary, dict):
        raise Failure(
            description="Scrapy crawl summary must be a JSON object",
            allow_retries=True,
        )
    return summary


def _ingestion_violations(summary: dict[str, Any]) -> list[str]:
    allowed_reasons = env_csv("DAGSTER_ALLOWED_CLOSE_REASONS", "finished")
    violations: list[str] = []
    request_failures = _summary_int(
        summary,
        "request_failed",
        violations,
    ) + _summary_int(
        summary,
        "search_request_failed",
        violations,
    )
    limits = {
        "request failures": (
            request_failures,
            env_int("DAGSTER_MAX_REQUEST_FAILURES", 0, minimum=0),
        ),
        "persistence errors": (
            _summary_int(summary, "persistence_errors", violations),
            env_int("DAGSTER_MAX_PERSISTENCE_ERRORS", 0, minimum=0),
        ),
        "unexplained missing documents": (
            _summary_int(summary, "unexplained_missing", violations),
            env_int("DAGSTER_MAX_UNEXPLAINED_MISSING", 0, minimum=0),
        ),
    }

    reason = summary.get("reason")
    if reason not in allowed_reasons:
        violations.append(f"unexpected Scrapy close reason: {reason!r}")

    for label, (actual, maximum) in limits.items():
        if actual > maximum:
            violations.append(f"{label}={actual} exceeds allowed {maximum}")
    return violations


def _require_persistence() -> None:
    if not env_bool("PERSISTENCE_ENABLED", False):
        raise Failure(
            description=(
                "Dagster assets require PERSISTENCE_ENABLED=true because "
                "their outputs are MongoDB records and MinIO objects."
            ),
            allow_retries=False,
        )


def _summary_int(
    summary: dict[str, Any],
    key: str,
    violations: list[str],
) -> int:
    value = summary.get(key, 0)
    if isinstance(value, bool) or not isinstance(value, int):
        violations.append(f"invalid {key} value in crawl summary")
        return 0
    return value


def _subprocess_tail(output: str | bytes | None) -> str:
    if not output:
        return ""
    text = (
        output.decode("utf-8", errors="replace")
        if isinstance(output, bytes)
        else output
    )
    line_count = env_int("DAGSTER_SUBPROCESS_LOG_TAIL_LINES", 80, minimum=1)
    character_limit = env_int(
        "DAGSTER_SUBPROCESS_LOG_TAIL_CHARACTERS",
        12_000,
        minimum=1,
    )
    tail = "\n".join(text.splitlines()[-line_count:])
    return tail[-character_limit:]


def _log_subprocess_tail(
    context: AssetExecutionContext,
    stream_name: str,
    output: str | bytes | None,
) -> None:
    tail = _subprocess_tail(output)
    if tail:
        context.log.error("Scrapy %s tail:\n%s", stream_name, tail)


document_pipeline_job = define_asset_job(
    name="document_pipeline_job",
    selection=AssetSelection.assets(raw_documents, scraped_documents),
    partitions_def=SOURCE_MONTH_PARTITIONS,
)


@schedule(
    job=document_pipeline_job,
    cron_schedule=os.getenv("DAGSTER_SCHEDULE_CRON", "0 2 1 * *"),
    execution_timezone=os.getenv(
        "DAGSTER_PARTITION_TIMEZONE",
        "Europe/Dublin",
    ),
)
def document_pipeline_schedule(context):
    """Launch the latest completed month once for every configured source."""
    date_key = MONTHLY_PARTITIONS.get_last_partition_key(
        current_time=context.scheduled_execution_time
    )
    if date_key is None:
        return SkipReason("No completed monthly partition is available")

    return [
        RunRequest(
            run_key=f"{source_key}:{date_key}",
            partition_key=MultiPartitionKey(
                {"date": date_key, "source": source_key}
            ),
        )
        for source_key in sorted(SOURCE_REGISTRY)
    ]


defs = Definitions(
    assets=[raw_documents, scraped_documents],
    jobs=[document_pipeline_job],
    schedules=[document_pipeline_schedule],
)
