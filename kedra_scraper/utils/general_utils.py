from __future__ import annotations

import json
import logging
from hashlib import sha256
import os
from pathlib import Path
from typing import Any

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError, TimeoutError
from twisted.python.failure import Failure

from kedra_scraper.utils.logging import (
    log_structured,
    record_body_metric,
)

def hash_document(content: str | bytes) -> str:
    """Return a SHA-256 hash for non-empty document content."""
    if isinstance(content, str):
        document_bytes = content.encode("utf-8")
    elif isinstance(content, bytes):
        document_bytes = content
    else:
        raise TypeError("document content must be str or bytes")

    if not document_bytes:
        raise ValueError("document content must not be empty")

    return sha256(document_bytes).hexdigest()


def handle_request_error(
    spider,
    failure: Failure,
    *,
    request_kind: str = "document",
) -> None:
    """Record a request after Scrapy has exhausted its HTTP retries."""
    if request_kind not in {"document", "search"}:
        raise ValueError(f"Unsupported request kind: {request_kind}")

    request = failure.request
    identifier = request.cb_kwargs.get("identifier")
    partition_date = request.cb_kwargs.get("partition_date")
    body = request.cb_kwargs.get("category")
    stats = spider.crawler.stats
    counter_prefix = "documents" if request_kind == "document" else "search"

    stats.inc_value(f"{counter_prefix}/request_failed")
    record_body_metric(
        spider,
        partition_date,
        body,
        "request_failures",
    )
    if request_kind == "document":
        record_body_metric(
            spider,
            partition_date,
            body,
            "failed",
        )

    status_code = None
    if failure.check(HttpError):
        status_code = failure.value.response.status
        error_type = f"http_{status_code}"
        stats.inc_value(f"errors/{error_type}")
    elif failure.check(DNSLookupError):
        error_type = "dns"
        stats.inc_value("errors/dns")
    elif failure.check(TimeoutError, TCPTimedOutError):
        error_type = "timeout"
        stats.inc_value("errors/timeout")
    else:
        error_type = type(failure.value).__name__
        stats.inc_value(f"errors/request/{error_type}")

    log_structured(
        spider.logger,
        logging.ERROR,
        f"{request_kind}_request_failed",
        f"{request_kind.capitalize()} request failed after retries",
        partition_date=partition_date,
        body=body,
        identifier=identifier,
        url=request.url,
        status_code=status_code,
        error_type=error_type,
        reason=failure.getErrorMessage(),
    )

def write_crawl_summary(path_value: str, summary: dict[str, Any]) -> None:
    """Atomically write the machine-readable crawl outcome for orchestration."""
    destination = Path(path_value).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f"{destination.name}.tmp")
    temporary.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(destination)



def env_int(
    name: str,
    default: int,
    *,
    minimum: int | None = None,
) -> int:
    """ Parses an environment variable into an int,
    falling back to default if unset, and optionally validating
    that it meets a minimum threshold."""
    raw_value = os.getenv(name)
    try:
        value = default if raw_value is None else int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc

    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def env_bool(name: str, default: bool) -> bool:
    """ Parses an environment variable into a bool,
    falling back to default if unset"""
    value = os.getenv(name)
    if value is None:
        return default

    normalised = value.strip().lower()
    if normalised in {"1", "true", "yes", "on"}:
        return True
    if normalised in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"{name} must be one of true/false, yes/no, on/off or 1/0"
    )



def env_float(
    name: str,
    default: float,
    *,
    minimum: float | None = None,
) -> float:
    """ Parses an environment variable into a float,
    falling back to default if unset, and optionally validating
    that it meets a minimum threshold."""
    raw_value = os.getenv(name)
    try:
        value = default if raw_value is None else float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc

    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value
