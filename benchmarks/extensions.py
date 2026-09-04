from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from numbers import Real
from typing import Any, Mapping, Sequence

from scrapy import signals
from scrapy.exceptions import NotConfigured

from kedra_scraper.utils import write_crawl_summary


PROFILE_SETTING_NAMES = (
    "CONCURRENT_REQUESTS",
    "CONCURRENT_REQUESTS_PER_DOMAIN",
    "DOWNLOAD_DELAY",
    "DOWNLOAD_TIMEOUT",
    "RANDOMIZE_DOWNLOAD_DELAY",
    "RETRY_TIMES",
    "RETRY_HTTP_CODES",
    "AUTOTHROTTLE_ENABLED",
    "AUTOTHROTTLE_START_DELAY",
    "AUTOTHROTTLE_MAX_DELAY",
    "AUTOTHROTTLE_TARGET_CONCURRENCY",
    "AUTOTHROTTLE_DEBUG",
    "CLOSESPIDER_ITEMCOUNT",
    "PERSISTENCE_ENABLED",
    "SCRAPE_MODE",
    "STRUCTURED_LOG_PATH",
    "STRUCTURED_LOG_MAX_BYTES",
    "STRUCTURED_LOG_BACKUP_COUNT",
)


class CrawlProfilerExtension:
    """Write performance and reliability statistics for an opt-in crawl."""

    def __init__(self, crawler, destination: str) -> None:
        self.crawler = crawler
        self.destination = destination
        self.started_at = 0.0
        self.latencies: list[float] = []
        self.successful_retry_responses = 0

        crawler.signals.connect(self.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(
            self.response_received,
            signal=signals.response_received,
        )
        crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)

    @classmethod
    def from_crawler(cls, crawler):
        destination = crawler.settings.get("CRAWL_PROFILE_PATH")
        if not destination:
            raise NotConfigured("CRAWL_PROFILE_PATH is not configured")
        return cls(crawler, destination)

    def spider_opened(self, spider) -> None:
        self.started_at = time.perf_counter()

    def response_received(self, response, request, spider) -> None:
        latency = request.meta.get("download_latency")
        if (
            isinstance(latency, Real)
            and not isinstance(latency, bool)
            and latency >= 0
        ):
            self.latencies.append(float(latency))

        retry_times = request.meta.get("retry_times", 0)
        if retry_times and 200 <= response.status < 400:
            self.successful_retry_responses += 1

    def spider_closed(self, spider, reason: str) -> None:
        duration = max(time.perf_counter() - self.started_at, 0.0)
        settings_snapshot = {
            name: self.crawler.settings.get(name)
            for name in PROFILE_SETTING_NAMES
        }
        profile = build_crawl_profile(
            spider_name=spider.name,
            source=self.crawler.settings.get("CRAWL_PROFILE_SOURCE") or None,
            close_reason=reason,
            duration_seconds=duration,
            stats=self.crawler.stats.get_stats(),
            settings=settings_snapshot,
            latencies=self.latencies,
            successful_retry_responses=self.successful_retry_responses,
        )
        write_crawl_summary(self.destination, profile)
        spider.logger.info("Crawl profile written to %s", self.destination)


def build_crawl_profile(
    *,
    spider_name: str,
    source: str | None,
    close_reason: str,
    duration_seconds: float,
    stats: Mapping[str, Any],
    settings: Mapping[str, Any],
    latencies: Sequence[float],
    successful_retry_responses: int,
) -> dict[str, Any]:
    """Build the serialisable profile independently of Scrapy's runtime."""
    expected = _count(stats, "documents/expected")
    scraped = _count(stats, "item_scraped_count")
    request_failed = _count(stats, "documents/request_failed")
    search_request_failed = _count(stats, "search/request_failed")
    incomplete_results = _count(stats, "documents/incomplete_result")
    dropped = _count(stats, "item_dropped_count")
    extraction_failed = sum(
        _count(stats, key)
        for key in (
            "errors/html_parsing",
            "errors/html_empty",
            "errors/pdf_parsing",
            "errors/pdf_empty",
            "errors/content_hash",
        )
    )
    missing = max(expected - scraped, 0)
    explained_missing = min(
        request_failed + incomplete_results + extraction_failed + dropped,
        missing,
    )

    duration = max(float(duration_seconds), 0.0)
    throughput = (scraped * 60.0 / duration) if duration else 0.0
    status_counts = _prefixed_counts(
        stats,
        "downloader/response_status_count/",
    )
    response_count = _count(stats, "downloader/response_count")
    status_rates = {
        status: _rate(count, response_count)
        for status, count in status_counts.items()
    }
    recovered_chains = successful_retry_responses
    exhausted_chains = _count(stats, "retry/max_reached")
    retry_chains = recovered_chains + exhausted_chains
    completeness_evaluated = close_reason == "finished"

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "spider": spider_name,
        "close_reason": close_reason,
        "duration_seconds": round(duration, 3),
        "settings": dict(settings),
        "requests": {
            "sent": _count(stats, "downloader/request_count"),
            "responses": response_count,
            "response_bytes": _count(stats, "downloader/response_bytes"),
            "http_status_counts": status_counts,
            "http_status_rates": status_rates,
            "notable_status_rates": {
                "403": status_rates.get("403", 0.0),
                "429": status_rates.get("429", 0.0),
                "5xx": _rate(
                    sum(
                        count
                        for status, count in status_counts.items()
                        if status.isdigit() and 500 <= int(status) <= 599
                    ),
                    response_count,
                ),
            },
        },
        "documents": {
            "expected": expected,
            "scheduled": _count(stats, "documents/scheduled"),
            "scraped": scraped,
            "throughput_per_minute": round(throughput, 3),
            "completeness_evaluated": completeness_evaluated,
            "missing": missing,
            "explained_missing": explained_missing,
            "unexplained_missing": (
                max(missing - explained_missing, 0)
                if completeness_evaluated
                else None
            ),
            "html_extracted": _count(stats, "documents/html_extracted"),
            "pdf_extracted": _count(stats, "documents/pdf_extracted"),
            "html_ingested": _count(stats, "documents/html_ingested"),
            "pdf_ingested": _count(stats, "documents/pdf_ingested"),
        },
        "latency_seconds": latency_summary(latencies),
        "retries": {
            "attempts": _count(stats, "retry/count"),
            "successful_responses_after_retry": recovered_chains,
            "exhausted": exhausted_chains,
            "recovery_rate": (
                _rate(recovered_chains, retry_chains)
                if retry_chains
                else None
            ),
            "reason_counts": _prefixed_counts(stats, "retry/reason_count/"),
        },
        "failures": {
            "document_requests": request_failed,
            "search_requests": search_request_failed,
            "extraction": extraction_failed,
            "incomplete_results": incomplete_results,
            "dropped_items": dropped,
            "error_counts": _prefixed_counts(stats, "errors/"),
        },
    }


def latency_summary(values: Sequence[float]) -> dict[str, int | float | None]:
    """Summarise observed network response latency in seconds."""
    samples = sorted(float(value) for value in values if value >= 0)
    if not samples:
        return {
            "samples": 0,
            "minimum": None,
            "mean": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "maximum": None,
        }

    return {
        "samples": len(samples),
        "minimum": round(samples[0], 4),
        "mean": round(sum(samples) / len(samples), 4),
        "p50": round(_percentile(samples, 50), 4),
        "p95": round(_percentile(samples, 95), 4),
        "p99": round(_percentile(samples, 99), 4),
        "maximum": round(samples[-1], 4),
    }


def _percentile(sorted_values: Sequence[float], percentile: float) -> float:
    if not sorted_values:
        raise ValueError("Cannot calculate a percentile without values")
    position = (len(sorted_values) - 1) * percentile / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(sorted_values[lower])
    fraction = position - lower
    return (
        float(sorted_values[lower]) * (1.0 - fraction)
        + float(sorted_values[upper]) * fraction
    )


def _count(stats: Mapping[str, Any], key: str) -> int:
    value = stats.get(key, 0)
    return int(value) if isinstance(value, Real) else 0


def _prefixed_counts(stats: Mapping[str, Any], prefix: str) -> dict[str, int]:
    return {
        key.removeprefix(prefix): _count(stats, key)
        for key in sorted(stats)
        if key.startswith(prefix)
    }


def _rate(count: int, total: int) -> float:
    return round(count / total, 6) if total else 0.0
