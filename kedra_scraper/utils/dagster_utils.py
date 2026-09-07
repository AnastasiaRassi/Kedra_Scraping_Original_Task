from __future__ import annotations

import json
import os
import subprocess
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from dagster import (
    AssetExecutionContext,
    Backoff,
    Failure,
    MultiPartitionKey,
    RetryPolicy,
)

from kedra_scraper.source_registry import (
    DEFAULT_SOURCE_REGISTRY_PATH,
    SourceRegistryEntry,
    load_source_registry,
)
from kedra_scraper.utils.helpers import env_bool, env_float, env_int

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

SOURCE_REGISTRY = load_source_registry(
    os.getenv("SOURCE_REGISTRY_PATH", DEFAULT_SOURCE_REGISTRY_PATH)
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


def partition_context(
    context: AssetExecutionContext,
    monthly_partitions_def: Any,
) -> tuple[SourceRegistryEntry, str, date, date]:
    """ This helper function extracts and validates the partition parameters (source and date) for the 
    current Dagster asset execution run."""
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

    window = monthly_partitions_def.time_window_for_partition_key(date_key)
    start_date = window.start.date()
    end_date = (window.end - timedelta(days=1)).date()
    return source_config, start_date.isoformat(), start_date, end_date


def dagster_structured_log_path(
    source: str,
    partition_date: str,
    run_id: str,
) -> Path:
    """Return the retained Scrapy log path for one Dagster asset run."""
    root = Path(os.getenv("SCRAPY_LOG_DIR", "logs")).expanduser()
    if not root.is_absolute():
        root = PROJECT_ROOT / root
    safe_source = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in source
    )
    safe_run_id = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in run_id
    )
    return (
        root
        / "dagster"
        / (safe_source or "source")
        / partition_date
        / f"raw_documents_{safe_run_id or 'run'}.jsonl"
    )


def dagster_scraping_summary_path(
    source: str,
    partition_date: str,
    run_id: str,
) -> Path:
    """Return the path for scraping summary JSON files."""
    log_path = dagster_structured_log_path(source, partition_date, run_id)
    return log_path.with_name(
        log_path.name.replace("raw_documents_", "scraped_documents_").replace(
            ".jsonl",
            ".summary.json",
        )
    )


def run_scrapy(
    context: AssetExecutionContext,
    command: list[str],
    *,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    """Execute a Scrapy subprocess in the project root, capturing output and enforcing timeouts.

    Runs the specified CLI command ( like python -m scrapy crawl <spider) with full environment 
    inheritance. Catches process timeouts and non-zero exit codes, logs stdout/stderr tails to the 
    Dagster context, and raises a retryable Dagster Failure.
    """
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
        log_subprocess_tail(context, "stdout", exc.stdout)
        log_subprocess_tail(context, "stderr", exc.stderr)
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
        stdout_tail = subprocess_tail(result.stdout)
        stderr_tail = subprocess_tail(result.stderr)
        log_subprocess_tail(context, "stdout", stdout_tail)
        log_subprocess_tail(context, "stderr", stderr_tail)

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


def read_summary(path: Path) -> dict[str, Any]:
    """Read and validate the Scrapy crawl summary JSON output."""
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


def ingestion_violations(summary: dict[str, Any]) -> list[str]:
    """Check summary metrics against configured quality thresholds."""
    violations: list[str] = []
    request_failures = summary_int(
        summary,
        "request_failed",
        violations,
    ) + summary_int(
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
            summary_int(summary, "persistence_errors", violations),
            env_int("DAGSTER_MAX_PERSISTENCE_ERRORS", 0, minimum=0),
        ),
        "unexplained missing documents": (
            summary_int(summary, "unexplained_missing", violations),
            env_int("DAGSTER_MAX_UNEXPLAINED_MISSING", 0, minimum=0),
        ),
    }

    reason = summary.get("reason")
    if reason != "finished":
        violations.append(f"unexpected Scrapy close reason: {reason!r}")

    for label, (actual, maximum) in limits.items():
        if actual > maximum:
            violations.append(f"{label}={actual} exceeds allowed {maximum}")
    return violations


def require_persistence() -> None:
    """Ensure persistence env flag is enabled before running Dagster assets."""
    if not env_bool("PERSISTENCE_ENABLED", False):
        raise Failure(
            description=(
                "Dagster assets require PERSISTENCE_ENABLED=true because "
                "their outputs are MongoDB records and MinIO objects."
            ),
            allow_retries=False,
        )


def summary_int(
    summary: dict[str, Any],
    key: str,
    violations: list[str],
) -> int:
    value = summary.get(key, 0)
    if isinstance(value, bool) or not isinstance(value, int):
        violations.append(f"invalid {key} value in crawl summary")
        return 0
    return value


def subprocess_tail(output: str | bytes | None) -> str:
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


def log_subprocess_tail(
    context: AssetExecutionContext,
    stream_name: str,
    output: str | bytes | None,
) -> None:
    tail = subprocess_tail(output)
    if tail:
        context.log.error("Scrapy %s tail:\n%s", stream_name, tail)
