from __future__ import annotations

import json
import logging
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from kedra_scraper.structured_logging import (
    JsonLogFormatter,
    StructuredLoggingExtension,
    body_partition_summaries,
    default_structured_log_path,
    log_structured,
    record_body_metric,
)


class _Stats:
    def __init__(self) -> None:
        self.values: dict[str, int | str] = {}

    def inc_value(self, key: str, count: int = 1) -> None:
        self.values[key] = int(self.values.get(key, 0)) + count

    def set_value(self, key: str, value) -> None:
        self.values[key] = value


class _Settings(dict):
    def getint(self, key: str, default: int = 0) -> int:
        return int(self.get(key, default))


class _Signals:
    def connect(self, receiver, signal) -> None:
        pass


class _Crawler:
    def __init__(self, settings: dict | None = None) -> None:
        self.stats = _Stats()
        self.settings = _Settings(settings or {})
        self.signals = _Signals()


class _Spider:
    name = "test/spider"

    def __init__(self, crawler: _Crawler | None = None) -> None:
        self.crawler = crawler or _Crawler()
        self.logger = logging.getLogger("test.structured.spider")
        self.logger.setLevel(logging.INFO)


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

    def test_default_path_is_safe_and_deterministic(self) -> None:
        path = default_structured_log_path(
            "logs",
            "test/spider",
            timestamp=datetime(2026, 9, 4, 8, 0, tzinfo=timezone.utc),
            process_id=42,
        )

        self.assertEqual(
            path,
            Path("logs/test_spider/20260904T080000Z_42.jsonl"),
        )

    def test_extension_retains_and_closes_jsonl_log(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "crawl.jsonl"
            crawler = _Crawler(
                {
                    "STRUCTURED_LOG_PATH": str(path),
                    "STRUCTURED_LOG_MAX_BYTES": 1_000_000,
                    "STRUCTURED_LOG_BACKUP_COUNT": 1,
                    "LOG_LEVEL": "INFO",
                }
            )
            spider = _Spider(crawler)
            extension = StructuredLoggingExtension(crawler)

            extension.spider_opened(spider)
            log_structured(
                spider.logger,
                logging.WARNING,
                "test_event",
                "Retained event",
                url="https://example.test/document",
            )
            extension.spider_closed(spider, "finished")

            rows = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(
                [row["event"] for row in rows],
                [
                    "structured_log_started",
                    "test_event",
                    "structured_log_closed",
                ],
            )
            self.assertEqual(rows[1]["url"], "https://example.test/document")
            self.assertIsNone(extension.file_handler)
            self.assertEqual(
                crawler.stats.values["logging/structured_log_path"],
                str(path),
            )

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
