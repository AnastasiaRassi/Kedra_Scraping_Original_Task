from __future__ import annotations

import unittest

from benchmarks.crawl_profiler import parse_assignments
from kedra_scraper.extensions import build_crawl_profile, latency_summary


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

    def test_latency_summary_handles_empty_and_small_samples(self) -> None:
        self.assertIsNone(latency_summary([])["p95"])
        summary = latency_summary([1.0, 2.0, 3.0, 4.0])
        self.assertEqual(summary["p50"], 2.5)
        self.assertEqual(summary["p95"], 3.85)

    def test_assignment_parser_accepts_values_containing_equals(self) -> None:
        self.assertEqual(
            parse_assignments(["query=a=b", "limit=10"], "argument"),
            {"query": "a=b", "limit": "10"},
        )

    def test_assignment_parser_rejects_invalid_input(self) -> None:
        with self.assertRaises(SystemExit):
            parse_assignments(["missing-separator"], "argument")


if __name__ == "__main__":
    unittest.main()
