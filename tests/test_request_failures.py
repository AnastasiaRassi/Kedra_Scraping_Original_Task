from __future__ import annotations

import logging
import unittest
from types import SimpleNamespace

from scrapy.downloadermiddlewares.retry import get_retry_request
from scrapy.http import HtmlResponse, Request
from scrapy.settings import Settings
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError

from kedra_scraper.utils import handle_request_error


class _Stats:
    def __init__(self) -> None:
        self.values: dict[str, int] = {}

    def inc_value(
        self,
        key: str,
        count: int = 1,
        start: int = 0,
    ) -> None:
        self.values[key] = self.values.get(key, start) + count


class _Logger:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def log(self, level: int, message: str, **kwargs) -> None:
        extra = kwargs.get("extra", {})
        self.events.append(
            {
                "level": level,
                "message": message,
                "event": extra.get("event"),
                **extra.get("structured_fields", {}),
            }
        )


class _Spider:
    name = "failure_test"

    def __init__(self) -> None:
        self.logger = _Logger()
        self.crawler = SimpleNamespace(
            stats=_Stats(),
            settings=Settings(
                {
                    "RETRY_TIMES": 3,
                    "RETRY_PRIORITY_ADJUST": 0,
                }
            ),
        )


class _Failure:
    def __init__(self, request: Request, value: Exception) -> None:
        self.request = request
        self.value = value

    def check(self, *error_types):
        return next(
            (
                error_type
                for error_type in error_types
                if isinstance(self.value, error_type)
            ),
            None,
        )

    def getErrorMessage(self) -> str:
        return str(self.value)


def _document_request(url: str) -> Request:
    return Request(
        url,
        meta={"retry_times": 3},
        cb_kwargs={
            "identifier": "TEST-001",
            "partition_date": "2026-08-01",
            "category": "Test body",
        },
    )


class RetryInjectionTests(unittest.TestCase):
    def test_three_retries_then_exhaustion(self) -> None:
        spider = _Spider()
        request = Request("https://example.test/document")

        retries = []
        for retry_number in range(1, 4):
            request = get_retry_request(
                request,
                spider=spider,
                reason=TimeoutError("injected timeout"),
                max_retry_times=3,
                priority_adjust=0,
            )
            self.assertIsNotNone(request)
            self.assertEqual(request.meta["retry_times"], retry_number)
            retries.append(request)

        exhausted = get_retry_request(
            retries[-1],
            spider=spider,
            reason=TimeoutError("injected timeout"),
            max_retry_times=3,
            priority_adjust=0,
        )

        self.assertIsNone(exhausted)
        self.assertEqual(spider.crawler.stats.values["retry/count"], 3)
        self.assertEqual(spider.crawler.stats.values["retry/max_reached"], 1)

    def test_exhausted_404_records_exact_context(self) -> None:
        spider = _Spider()
        request = _document_request("https://example.test/missing")
        response = HtmlResponse(
            request.url,
            status=404,
            request=request,
        )
        failure = _Failure(
            request,
            HttpError(response, "Ignoring non-200 response"),
        )

        handle_request_error(spider, failure)

        stats = spider.crawler.stats.values
        self.assertEqual(stats["documents/request_failed"], 1)
        self.assertEqual(stats["errors/http_404"], 1)
        self.assertEqual(
            stats[
                "body_partition/2026-08-01/Test body/request_failures"
            ],
            1,
        )
        self.assertEqual(
            stats["body_partition/2026-08-01/Test body/failed"],
            1,
        )

        event = spider.logger.events[-1]
        self.assertEqual(event["level"], logging.ERROR)
        self.assertEqual(event["event"], "document_request_failed")
        self.assertEqual(event["identifier"], "TEST-001")
        self.assertEqual(event["url"], request.url)
        self.assertEqual(event["status_code"], 404)
        self.assertEqual(event["error_type"], "http_404")
        self.assertEqual(event["reason"], "Ignoring non-200 response")

    def test_exhausted_timeout_records_failure_without_status(self) -> None:
        spider = _Spider()
        request = _document_request("https://example.test/slow")
        failure = _Failure(
            request,
            TimeoutError("injected timeout after 30 seconds"),
        )

        expected_reason = failure.getErrorMessage()
        handle_request_error(spider, failure)

        stats = spider.crawler.stats.values
        self.assertEqual(stats["documents/request_failed"], 1)
        self.assertEqual(stats["errors/timeout"], 1)

        event = spider.logger.events[-1]
        self.assertEqual(event["event"], "document_request_failed")
        self.assertEqual(event["url"], request.url)
        self.assertIsNone(event["status_code"])
        self.assertEqual(event["error_type"], "timeout")
        self.assertEqual(event["reason"], expected_reason)
        self.assertIn("injected timeout", event["reason"])


if __name__ == "__main__":
    unittest.main()
