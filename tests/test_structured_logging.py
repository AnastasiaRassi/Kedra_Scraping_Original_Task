from __future__ import annotations

import json
import logging
import unittest

from kedra_scraper.structured_logging import (
    JsonLogFormatter,
    body_partition_summaries,
    record_body_metric,
)


class _Stats:
    def __init__(self) -> None:
        self.values: dict[str, int] = {}

    def inc_value(self, key: str, count: int = 1) -> None:
        self.values[key] = self.values.get(key, 0) + count


class _Crawler:
    def __init__(self) -> None:
        self.stats = _Stats()


class _Spider:
    def __init__(self) -> None:
        self.crawler = _Crawler()


class StructuredLoggingTests(unittest.TestCase):
    def test_formatter_emits_valid_json_with_context(self) -> None:
        record = logging.LogRecord(
            name="test.spider",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="Download failed",
            args=(),
            exc_info=None,
        )
        record.event = "document_request_failed"
        record.structured_fields = {
            "partition_date": "2026-08-01",
            "body": "Labour Court",
            "url": "https://example.test/document",
            "status_code": 404,
            "reason": "Not Found",
        }

        payload = json.loads(JsonLogFormatter().format(record))

        self.assertEqual(payload["level"], "ERROR")
        self.assertEqual(payload["event"], "document_request_failed")
        self.assertEqual(payload["partition_date"], "2026-08-01")
        self.assertEqual(payload["body"], "Labour Court")
        self.assertEqual(payload["status_code"], 404)

    def test_body_partition_metrics_are_reconciled(self) -> None:
        spider = _Spider()

        record_body_metric(
            spider,
            "2026-08-01",
            "Labour Court",
            "found",
            count=3,
        )
        record_body_metric(
            spider,
            "2026-08-01",
            "Labour Court",
            "succeeded",
            count=2,
        )
        record_body_metric(
            spider,
            "2026-08-01",
            "Labour Court",
            "failed",
        )

        self.assertEqual(
            body_partition_summaries(spider),
            [
                {
                    "partition_date": "2026-08-01",
                    "body": "Labour Court",
                    "found": 3,
                    "succeeded": 2,
                    "failed": 1,
                    "request_failures": 0,
                }
            ],
        )
        self.assertEqual(
            spider.crawler.stats.values[
                "body_partition/2026-08-01/Labour Court/found"
            ],
            3,
        )


if __name__ == "__main__":
    unittest.main()
