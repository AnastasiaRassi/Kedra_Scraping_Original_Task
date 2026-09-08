from __future__ import annotations

import logging
import re
from datetime import datetime
from urllib.parse import urlsplit

import scrapy
from scrapy.utils.sitemap import Sitemap

from kedra_scraper.source_registry import apply_source_settings
from kedra_scraper.spiders.gb.sc_gb_site import (
    UKSCSourceConfig,
    load_uksc_source_config,
)
from kedra_scraper.items import KedraRawDocumentItem
from kedra_scraper.utils.structured_logging import (
    body_partition_summaries,
    log_structured,
    record_body_metric,
)
from kedra_scraper.utils import (
    handle_request_error as record_request_error,
    write_crawl_summary,
)


class UKSC_GB_Spider(scrapy.Spider):
    """Crawls UKSC judgments via the sitemap; the listing's pagination is JS-only."""

    name = "UKSC_GB"
    source_key = "uksc_gb"
    CATEGORY = "judgment"

    @classmethod
    def update_settings(cls, settings) -> None:
        super().update_settings(settings)
        apply_source_settings(settings, cls.source_key)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider._configure(crawler.settings)
        return spider

    def _configure(self, settings) -> None:
        """Load source rules before crawling."""
        config_path = settings.get("SITE_CONFIG_PATH")
        if not config_path:
            raise ValueError("SITE_CONFIG_PATH must not be empty")

        self.source_config: UKSCSourceConfig = load_uksc_source_config(
            config_path
        )
        self.allowed_domains = list(self.source_config.allowed_domains)
        self.source = self.source_config.source
        self.case_url_pattern = re.compile(
            self.source_config.case_url_pattern
        )

    async def start(self):
        yield scrapy.Request(
            url=self.source_config.sitemap_url,
            callback=self.parse_sitemap,
            errback=self.handle_search_request_error,
        )

    def parse_sitemap(self, response):
        """Schedule one request per case page listed in the sitemap."""
        case_urls = [
            entry["loc"]
            for entry in Sitemap(response.body)
            if entry.get("loc")
            and self.case_url_pattern.search(entry["loc"])
        ]
        self.crawler.stats.inc_value("cases/discovered", count=len(case_urls))

        if not case_urls:
            log_structured(
                self.logger,
                logging.ERROR,
                "empty_sitemap",
                "Sitemap listed no case pages",
                url=response.url,
                status_code=response.status,
                error_type="no_case_urls",
                reason="No sitemap entry matched the configured case pattern",
            )
            return

        for case_url in case_urls:
            yield scrapy.Request(
                url=case_url,
                callback=self.parse_case,
                errback=self.handle_request_error,
            )

    def parse_case(self, response):
        """Read one case page and schedule its judgment documents."""
        selectors = self.source_config.selectors

        identifier = self._selector_text(
            response.xpath(selectors.case_id)
        ) or None
        title = self._selector_text(response.xpath(selectors.title))
        status = self._selector_text(response.xpath(selectors.case_status))
        published_date = self._normalise_published_date(
            self._selector_text(response.xpath(selectors.judgment_date))
        )

        pdf_href = response.xpath(selectors.judgment_pdf).get()
        html_href = response.xpath(selectors.judgment_html_link).get()
        targets = [
            (response.urljoin(href), source_format)
            for href, source_format in (
                (pdf_href, "pdf"),
                (html_href, "html"),
            )
            if href
        ]

        if not targets:
            self.crawler.stats.inc_value("cases/without_judgment")
            self.logger.debug(
                "No judgment published yet for %s (status=%r)",
                response.url,
                status,
            )
            return

        if not title or not published_date:
            self.crawler.stats.inc_value("documents/incomplete_result")
            log_structured(
                self.logger,
                logging.WARNING,
                "incomplete_case_page",
                "Skipping a case page with incomplete metadata",
                body=self.CATEGORY,
                identifier=identifier,
                url=response.url,
                status_code=response.status,
                error_type="incomplete_metadata",
                reason="Missing case title or judgment date",
            )
            return

        partition_date = self._partition_date(published_date)
        metadata = self._case_metadata(response)
        metadata["case_status"] = status
        description = metadata.get("issue") or None

        record_body_metric(
            self,
            partition_date,
            self.CATEGORY,
            "found",
            count=len(targets),
        )

        for document_url, source_format in targets:
            self.crawler.stats.inc_value("documents/expected")
            yield scrapy.Request(
                url=document_url,
                callback=self.parse_document,
                errback=self.handle_request_error,
                cb_kwargs={
                    "title": title,
                    "identifier": identifier,
                    "published_date": published_date,
                    "partition_date": partition_date,
                    "category": self.CATEGORY,
                    "description": description,
                    "landing_url": response.url,
                    "source_metadata": metadata,
                    "source_format": source_format,
                },
            )

    def parse_document(
        self,
        response,
        title: str,
        identifier: str | None,
        published_date: str,
        partition_date: str,
        category: str,
        description: str | None,
        landing_url: str,
        source_metadata: dict,
        source_format: str,
    ):
        """Store one judgment document's raw bytes."""
        content_type = response.headers.get(b"Content-Type", b"").lower()
        is_pdf = (
            b"application/pdf" in content_type
            or self._is_pdf_url(response.url)
        )
        resolved_format = "pdf" if is_pdf else source_format

        self.crawler.stats.inc_value(f"documents/{resolved_format}_ingested")
        yield KedraRawDocumentItem(
            title=title,
            published_date=published_date,
            partition_date=partition_date,
            identifier=identifier,
            source=self.source,
            category=category,
            source_format=resolved_format,
            doc_url=response.url,
            landing_url=landing_url,
            raw_content=response.body,
            description=description,
            source_metadata=source_metadata,
        )

    def _case_metadata(self, response) -> dict:
        """Collect the configured single- and multi-value case fields."""
        metadata: dict = {}

        for field, query in self.source_config.metadata_selectors.items():
            value = self._clean_text(response.xpath(query).get())
            if value:
                metadata[field] = value

        for field, query in self.source_config.metadata_list_selectors.items():
            values = [
                cleaned
                for raw in response.xpath(query).getall()
                if (cleaned := self._clean_text(raw))
            ]
            if values:
                metadata[field] = values

        return metadata

    def handle_request_error(self, failure) -> None:
        """Delegate request-failure recording to the shared utility."""
        record_request_error(self, failure)

    def handle_search_request_error(self, failure) -> None:
        """Record a sitemap request after retries are exhausted."""
        record_request_error(self, failure, request_kind="search")

    def closed(self, reason: str) -> None:
        """Log a reconciliation summary after all requests and items finish."""
        stats = self.crawler.stats

        expected = stats.get_value("documents/expected", 0)
        scraped = stats.get_value("item_scraped_count", 0)
        request_failed = stats.get_value("documents/request_failed", 0)
        search_request_failed = stats.get_value("search/request_failed", 0)
        incomplete_results = stats.get_value(
            "documents/incomplete_result",
            0,
        )
        dropped = stats.get_value("item_dropped_count", 0)
        missing = max(expected - scraped, 0)
        explained_missing = request_failed + dropped
        unexplained_missing = max(missing - explained_missing, 0)

        persistence_errors = sum(
            stats.get_value(key, 0)
            for key in (
                "errors/persistence_identity",
                "errors/minio_payload",
                "errors/minio_write",
                "errors/mongodb_payload",
                "errors/mongodb_write",
            )
        )

        summary = {
            "reason": reason,
            "cases_discovered": stats.get_value("cases/discovered", 0),
            "cases_without_judgment": stats.get_value(
                "cases/without_judgment",
                0,
            ),
            "expected": expected,
            "scraped": scraped,
            "request_failed": request_failed,
            "search_request_failed": search_request_failed,
            "incomplete_results": incomplete_results,
            "dropped": dropped,
            "missing": missing,
            "unexplained_missing": unexplained_missing,
            "persistence_errors": persistence_errors,
            "persistence_retry_attempts": stats.get_value(
                "persistence/retry_attempts",
                0,
            ),
            "persistence_retry_recovered": stats.get_value(
                "persistence/retry_recovered",
                0,
            ),
            "persistence_retry_exhausted": stats.get_value(
                "persistence/retry_exhausted",
                0,
            ),
            "html_ingested": stats.get_value("documents/html_ingested", 0),
            "pdf_ingested": stats.get_value("documents/pdf_ingested", 0),
            "minio_inserted": stats.get_value(
                "persistence/minio_inserted",
                0,
            ),
            "minio_overwritten": stats.get_value(
                "persistence/minio_overwritten",
                0,
            ),
            "minio_unchanged": stats.get_value(
                "persistence/minio_unchanged",
                0,
            ),
            "mongodb_inserted": stats.get_value(
                "persistence/mongodb_inserted",
                0,
            ),
            "mongodb_updated": stats.get_value(
                "persistence/mongodb_updated",
                0,
            ),
            "mongodb_unchanged": stats.get_value(
                "persistence/mongodb_unchanged",
                0,
            ),
            "body_partitions": body_partition_summaries(self),
        }

        summary_path = self.settings.get("CRAWL_SUMMARY_PATH")
        if summary_path:
            try:
                write_crawl_summary(summary_path, summary)
            except (OSError, TypeError, ValueError):
                self.logger.exception(
                    "Could not write crawl summary to %s",
                    summary_path,
                )

        for body_summary in summary["body_partitions"]:
            log_structured(
                self.logger,
                logging.INFO,
                "body_partition_summary",
                "Body partition completed",
                **body_summary,
            )

        log_structured(
            self.logger,
            logging.INFO,
            "crawl_summary",
            "Crawl completed",
            **summary,
        )

        if missing:
            log_structured(
                self.logger,
                logging.WARNING,
                "crawl_incomplete",
                "Crawl finished with missing documents",
                expected=expected,
                scraped=scraped,
                missing=missing,
                explained_missing=min(explained_missing, missing),
                unexplained_missing=unexplained_missing,
            )

    @classmethod
    def _selector_text(cls, selector) -> str:
        """Extract readable text from an element, stripping tags and whitespace."""
        if selector is None:
            return ""
        return cls._clean_text(" ".join(selector.getall()))

    @staticmethod
    def _clean_text(value: str | None) -> str:
        return " ".join((value or "").split())

    def _normalise_published_date(self, value: str) -> str:
        cleaned_value = self._clean_text(value)

        for date_format in self.source_config.published_date_formats:
            try:
                return datetime.strptime(
                    cleaned_value,
                    date_format,
                ).date().isoformat()
            except ValueError:
                continue

        return cleaned_value

    @staticmethod
    def _partition_date(published_date: str) -> str:
        """Return the first day of the month a judgment was published in."""
        try:
            return datetime.strptime(published_date, "%Y-%m-%d").date().replace(
                day=1
            ).isoformat()
        except ValueError:
            return published_date

    @staticmethod
    def _is_pdf_url(url: str) -> bool:
        return urlsplit(url).path.lower().endswith(".pdf")
