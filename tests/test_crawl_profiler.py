from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path

from benchmarks.crawl_profiler import (
    aggregate_run_reports,
    month_partition_keys,
    normalise_status,
    partition_tags,
    read_max_concurrent_runs,
    read_structured_log,
)
from benchmarks.extensions import build_crawl_profile, latency_summary
from benchmarks.reproducibility import capture_reproducibility_metadata


class CrawlProfileTests(unittest.TestCase):
    def test_profile_calculates_reliability_and_performance_metrics(self) -> None:
        profile = build_crawl_profile(
            spider_name="example",
            source="example_source",
            close_reason="finished",
            duration_seconds=120,
            stats={
                "documents/expected": 65,
                "documents/scheduled": 64,
                "item_scraped_count": 60,
                "documents/request_failed": 1,
                "documents/incomplete_result": 1,
                "errors/html_empty": 1,
                "item_dropped_count": 1,
                "downloader/request_count": 70,
                "downloader/response_count": 68,
                "downloader/response_bytes": 1234,
                "downloader/response_status_count/200": 66,
                "downloader/response_status_count/503": 2,
                "retry/count": 2,
                "retry/reason_count/503 Service Unavailable": 2,
                "retry/max_reached": 1,
                "persistence/minio_uploaded": 3,
                "persistence/mongodb_inserted": 2,
                "persistence/mongodb_updated": 1,
                "body_partition/2026-01-01/Labour Court/found": 5,
                "body_partition/2026-01-01/Labour Court/succeeded": 4,
                "body_partition/2026-01-01/Labour Court/failed": 1,
            },
            settings={"DOWNLOAD_TIMEOUT": 30.0, "RETRY_TIMES": 3},
            latencies=[1.0, 2.0, 3.0, 4.0],
            successful_retry_responses=1,
        )

        self.assertEqual(profile["documents"]["throughput_per_minute"], 30.0)
        self.assertEqual(profile["documents"]["missing"], 5)
        self.assertEqual(profile["documents"]["explained_missing"], 4)
        self.assertEqual(profile["documents"]["unexplained_missing"], 1)
        self.assertEqual(profile["requests"]["http_status_counts"]["503"], 2)
        self.assertEqual(
            profile["requests"]["notable_status_rates"]["5xx"],
            0.029412,
        )
        self.assertEqual(profile["retries"]["attempts"], 2)
        self.assertEqual(
            profile["retries"]["successful_responses_after_retry"],
            1,
        )
        self.assertEqual(profile["failures"]["extraction"], 1)
        self.assertEqual(profile["retries"]["recovery_rate"], 0.5)
        self.assertEqual(profile["persistence"]["minio_uploaded"], 3)
        self.assertEqual(profile["persistence"]["mongodb_inserted"], 2)
        self.assertEqual(
            profile["body_partitions"][0],
            {
                "partition_date": "2026-01-01",
                "body": "Labour Court",
                "found": 5,
                "succeeded": 4,
                "failed": 1,
                "request_failures": 0,
            },
        )

    def test_latency_summary_handles_empty_and_small_samples(self) -> None:
        self.assertIsNone(latency_summary([])["p95"])
        summary = latency_summary([1.0, 2.0, 3.0, 4.0])
        self.assertEqual(summary["p50"], 2.5)
        self.assertEqual(summary["p95"], 3.85)

    def test_reproducibility_metadata_captures_inputs_and_hashes_configs(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "source.json"
            requirements = root / "requirements.txt"
            config.write_text('{"source": "example"}\n', encoding="utf-8")
            requirements.write_text("Scrapy==2.18.0\n", encoding="utf-8")

            metadata = capture_reproducibility_metadata(
                project_root=root,
                spider="example",
                source="example_source",
                spider_args={
                    "start_date": "01-01-2026",
                    "end_date": "31-01-2026",
                },
                setting_overrides={
                    "DOWNLOAD_DELAY": "0.5",
                    "API_TOKEN": "do-not-store",
                },
                config_paths=["source.json"],
                with_persistence=False,
                max_items=None,
                log_level="INFO",
            )

        invocation = metadata["invocation"]
        self.assertEqual(
            invocation["spider_arguments"]["start_date"],
            "01-01-2026",
        )
        self.assertTrue(
            invocation["setting_overrides"]["API_TOKEN"]["redacted"]
        )
        self.assertNotIn(
            "do-not-store",
            str(metadata),
        )
        self.assertTrue(
            metadata["source_configuration"]["files"][0]["exists"]
        )
        self.assertEqual(
            len(metadata["source_configuration"]["files"][0]["sha256"]),
            64,
        )
        self.assertEqual(len(metadata["requirements"]["sha256"]), 64)
        self.assertFalse(metadata["response_replay"]["available"])

    def test_month_partition_keys_are_inclusive(self) -> None:
        self.assertEqual(
            month_partition_keys("2025-11-01", "2026-02-01"),
            ["2025-11-01", "2025-12-01", "2026-01-01", "2026-02-01"],
        )
        with self.assertRaises(SystemExit):
            month_partition_keys("2026-01-02", "2026-02-01")

    def test_partition_tags_encode_both_dimensions(self) -> None:
        self.assertEqual(
            partition_tags("2026-01-01", "wrc_ie", "benchmark-1"),
            {
                "dagster/partition": "2026-01-01|wrc_ie",
                "dagster/partition/date": "2026-01-01",
                "dagster/partition/source": "wrc_ie",
                "kedra/benchmark": "true",
                "kedra/benchmark_id": "benchmark-1",
            },
        )

    def test_aggregate_reports_cross_partition_throughput_and_failure(self) -> None:
        profile = {
            "requests": {
                "sent": 12,
                "responses": 11,
                "response_bytes": 1000,
                "http_status_counts": {"200": 11},
            },
            "documents": {
                "expected": 5,
                "scraped": 5,
                "missing": 0,
                "unexplained_missing": 0,
            },
            "latency_seconds": {
                "samples": 2,
                "minimum": 0.1,
                "mean": 0.2,
                "p50": 0.2,
                "p95": 0.3,
                "p99": 0.3,
                "maximum": 0.3,
            },
            "retries": {
                "attempts": 1,
                "successful_responses_after_retry": 1,
                "exhausted": 0,
                "reason_counts": {"timeout": 1},
            },
            "failures": {
                "document_requests": 0,
                "search_requests": 0,
                "extraction": 0,
                "incomplete_results": 0,
                "dropped_items": 0,
                "error_counts": {},
            },
            "persistence": {"errors": 0, "minio_uploaded": 5},
        }
        successful = {
            "final_status": "SUCCESS",
            "crawl_profile": profile,
            "scraping_summary": {
                "raw_documents": 5,
                "scraped": 4,
                "unchanged": 1,
                "scraping_failed": 0,
            },
            "queue_seconds": 1,
            "execution_seconds": 20,
            "total_seconds": 21,
            "poll_errors": [],
            "failure_event_count": 0,
            "raw_asset_retries_observed": 0,
        }
        failed = {
            **successful,
            "final_status": "FAILURE",
            "crawl_profile": None,
            "scraping_summary": None,
        }

        aggregate, reliability = aggregate_run_reports(
            [successful, failed],
            wall_duration_seconds=60,
            peak_active_runs=2,
            profiler_timed_out=False,
        )

        self.assertEqual(aggregate["runs"]["observed_peak_active"], 2)
        self.assertEqual(aggregate["documents"]["pipeline_throughput_per_minute"], 5.0)
        self.assertEqual(aggregate["retries"]["scrapy_recovered"], 1)
        self.assertFalse(reliability["passed"])
        self.assertIn("only 1 of 2 Dagster runs succeeded", reliability["violations"])

    def test_structured_failure_reader_keeps_exact_context(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "crawl.jsonl"
            records = [
                {"level": "INFO", "event": "crawl_summary"},
                {
                    "level": "ERROR",
                    "event": "search_request_failed",
                    "url": "https://example.test/search",
                    "status_code": 503,
                    "reason": "Service unavailable",
                },
            ]
            path.write_text(
                "\n".join(json.dumps(record) for record in records),
                encoding="utf-8",
            )
            details = read_structured_log(path, limit=10)

        self.assertEqual(details["crawl_summary_events"], 1)
        self.assertEqual(details["failure_event_count"], 1)
        self.assertEqual(details["failure_events"][0]["status_code"], 503)
        self.assertEqual(
            details["failure_events"][0]["url"],
            "https://example.test/search",
        )

    def test_instance_concurrency_and_status_parsing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dagster.yaml"
            path.write_text(
                "concurrency:\n  runs:\n    max_concurrent_runs: 3\n",
                encoding="utf-8",
            )
            self.assertEqual(read_max_concurrent_runs(path), 3)
        self.assertEqual(normalise_status("DagsterRunStatus.SUCCESS"), "SUCCESS")


if __name__ == "__main__":
    unittest.main()
