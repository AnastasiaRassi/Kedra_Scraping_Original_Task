from hashlib import sha256

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


def handle_request_error(spider, failure: Failure) -> None:
    """Record a document request that ultimately failed."""
    request = failure.request
    identifier = request.cb_kwargs.get("identifier", "unknown")
    stats = spider.crawler.stats

    stats.inc_value("documents/request_failed")

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
        "Document request failed: identifier=%s url=%s "
        "error_type=%s detail=%s",
        identifier,
        request.url,
        error_type,
        failure.getErrorMessage(),
    )
