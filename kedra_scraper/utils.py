from twisted.python.failure import Failure
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TimeoutError


def handle_request_error(self, failure: Failure):
    request = failure.request
    stats = self.crawler.stats

    if failure.check(HttpError):
        status = failure.value.response.status
        stats.inc_value(f"errors/http_{status}")

        self.logger.error(
            "Document request failed: status=%s url=%s",
            status,
            request.url,
        )

    elif failure.check(TimeoutError):
        stats.inc_value("errors/timeout")
        self.logger.error("Document timed out: url=%s", request.url)

    elif failure.check(DNSLookupError):
        stats.inc_value("errors/dns")
        self.logger.error("DNS lookup failed: url=%s", request.url)

    else:
        stats.inc_value("errors/unexpected_request")
        self.logger.error(
            "Unexpected request failure: url=%s error=%r",
            request.url,
            failure.value,
        )