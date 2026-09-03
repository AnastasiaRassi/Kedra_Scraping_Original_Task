from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError, TimeoutError
from twisted.python.failure import Failure


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
    identifier = request.cb_kwargs.get("identifier", "unknown")
    stats = spider.crawler.stats
    counter_prefix = "documents" if request_kind == "document" else "search"

    stats.inc_value(f"{counter_prefix}/request_failed")

    if failure.check(HttpError):
        status = failure.value.response.status
        error_type = f"http_{status}"
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

    spider.logger.error(
        "%s request failed after retries: identifier=%s url=%s "
        "error_type=%s detail=%s",
        request_kind.capitalize(),
        identifier,
        request.url,
        error_type,
        failure.getErrorMessage(),
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
