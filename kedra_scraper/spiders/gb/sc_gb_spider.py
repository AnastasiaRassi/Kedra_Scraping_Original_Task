from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Iterator
from urllib.parse import urlencode, urlsplit

import scrapy
from pypdf import PdfReader

from kedra_scraper.items import (
    KedraExtractedDocumentItem,
    KedraRawDocumentItem,
)
from kedra_scraper.source_registry import apply_source_settings
from kedra_scraper.spiders.gb.sc_uk_site import (
    UKSCSourceConfig,
    load_uksc_source_config,
)
from kedra_scraper.utils import (
    handle_request_error as record_request_error,
    hash_document,
    write_crawl_summary,
)
from kedra_scraper.utils.structured_logging import (
    body_partition_summaries,
    log_structured,
    record_body_metric,
)


class UKSC_GB_Spider(scrapy.Spider):
    name = "UKSC_GB"
    source_key = "uksc_gb"

    @classmethod
    def update_settings(cls, settings) -> None:
        super().update_settings(settings)
        apply_source_settings(settings, cls.source_key)

    def __init__(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._start_date_argument = start_date
        self._end_date_argument = end_date

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider._configure(crawler.settings)
        return spider

    def _configure(self, settings) -> None:
        """Load UKSC source configuration and runtime parameters."""
        config_path = settings.get("UKSC_CONFIG_PATH") or settings.get("WRC_CONFIG_PATH")
        if not config_path:
            raise ValueError("UKSC_CONFIG_PATH must not be empty")

        self.source_config: UKSCSourceConfig = load_uksc_source_config(config_path)
        self.allowed_domains = list(self.source_config.allowed_domains)
        self.search_url = self.source_config.search_url
        self.source = self.source_config.source
        self.partition_months = settings.getint("SCRAPE_PARTITION_MONTHS", 12)
        self.scrape_mode = settings.get("SCRAPE_MODE", "full")

        if self.partition_months < 1:
            raise ValueError("SCRAPE_PARTITION_MONTHS must be at least 1")
        if self.scrape_mode not in {"full", "ingestion"}:
            raise ValueError("SCRAPE_MODE must be either 'full' or 'ingestion'")

        start_value = self._start_date_argument or settings.get("SCRAPE_START_DATE")
        end_value = self._end_date_argument or settings.get("SCRAPE_END_DATE")

        if not start_value or not end_value:
            raise ValueError(
                "Both dates are required. Pass -a start_date and -a end_date "
                "or set SCRAPE_START_DATE and SCRAPE_END_DATE."
            )

        self.start_date = self._parse_scraped_date(start_value, "start_date")
        self.end_date = self._parse_scraped_date(end_value, "end_date")

        if self.start_date > self.end_date:
            raise ValueError("start_date must be earlier than or equal to end_date")

    async def start(self):
        """Yield partitioned GET search requests across year ranges."""
        for partition_start, partition_end in self._period_partitions(
            self.start_date,
            self.end_date,
            self.partition_months,
        ):
            partition_date = partition_start.isoformat()
            category = "judgments"

            record_body_metric(
                self,
                partition_date,
                category,
                "found",
                count=0,
            )

            # Build GET parameters for UKSC date-filtered queries
            params = {
                "year": partition_start.year,
                "from": partition_start.strftime("%Y-%m-%d"),
                "to": partition_end.strftime("%Y-%m-%d"),
            }
            url = f"{self.search_url}?{urlencode(params)}"

            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.handle_search_request_error,
                cb_kwargs={
                    "category": category,
                    "partition_date": partition_date,
                },
            )

    def parse(self, response, category: str, partition_date: str):
        """Parse judgment search results page and extract document links."""
        selectors = self.source_config.selectors
        result_cards = response.css(selectors.result_card)

        self.crawler.stats.inc_value("documents/expected", count=len(result_cards))
        record_body_metric(self, partition_date, category, "found", count=len(result_cards))

        if not result_cards:
            self.logger.debug("No UKSC judgments found at %s", response.url)

        for card in result_cards:
            document_href = card.css(selectors.document_link).get()
            identifier = self._selector_text(card.css(selectors.identifier)) or None
            published_date = self._normalise_published_date(
                self._selector_text(card.css(selectors.published_date))
            )
            description = self._selector_text(card.css(selectors.description)) or None
            title = "_".join(v for v in (identifier, description) if v)

            if not document_href or not title or not published_date:
                self.crawler.stats.inc_value("documents/incomplete_result")
                record_body_metric(self, partition_date, category, "failed")
                log_structured(
                    self.logger,
                    logging.WARNING,
                    "incomplete_search_result",
                    "Skipping an incomplete search result",
                    partition_date=partition_date,
                    body=category,
                    identifier=identifier,
                    url=response.urljoin(document_href) if document_href else response.url,
                    status_code=response.status,
                    error_type="incomplete_metadata",
                    reason="Missing document link, title, or published date",
                )
                continue

            self.crawler.stats.inc_value("documents/scheduled")
            document_url = response.urljoin(document_href)
            callback = (
                self.parse_pdf
                if self._is_pdf_url(document_url)
                else self.parse_document
            )

            yield scrapy.Request(
                url=document_url,
                callback=callback,
                errback=self.handle_request_error,
                cb_kwargs={
                    "title": title,
                    "identifier": identifier,
                    "published_date": published_date,
                    "partition_date": partition_date,
                    "category": category,
                    "description": description,
                    "landing_url": document_url,
                },
            )

        next_page = response.css(selectors.next_page).get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                errback=self.handle_search_request_error,
                cb_kwargs={
                    "category": category,
                    "partition_date": partition_date,
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
    ):
        """Parse HTML judgment page or forward to PDF parser if PDF found."""
        content_type = response.headers.get(b"Content-Type", b"").lower()
        if b"application/pdf" in content_type or self._is_pdf_url(response.url):
            yield from self.parse_pdf(
                response,
                title=title,
                identifier=identifier,
                published_date=published_date,
                partition_date=partition_date,
                category=category,
                description=description,
                landing_url=landing_url,
            )
            return

        pdf_href = response.css(self.source_config.selectors.pdf_download).get()
        pdf_url = response.urljoin(pdf_href) if pdf_href else None

        if pdf_url and self._is_pdf_url(pdf_url):
            yield scrapy.Request(
                url=pdf_url,
                callback=self.parse_pdf,
                errback=self.handle_request_error,
                cb_kwargs={
                    "title": title,
                    "identifier": identifier,
                    "published_date": published_date,
                    "partition_date": partition_date,
                    "category": category,
                    "description": description,
                    "landing_url": landing_url,
                },
            )
            return

        if self.scrape_mode == "ingestion":
            self.crawler.stats.inc_value("documents/html_ingested")
            yield KedraRawDocumentItem(
                title=title,
                published_date=published_date,
                partition_date=partition_date,
                identifier=identifier,
                source=self.source,
                category=category,
                source_format="html",
                doc_url=response.url,
                landing_url=landing_url,
                raw_content=response.body,
                description=description,
            )
            return

        content_node = None
        for selector in self.source_config.selectors.html_content:
            selected = response.css(selector)
            if selected:
                content_node = selected
                break
        content = self._selector_text(content_node)

        if not content:
            self.crawler.stats.inc_value("errors/html_empty")
            record_body_metric(self, partition_date, category, "failed")
            return

        content_hash = hash_document(content)
        self.crawler.stats.inc_value("documents/html_extracted")
        yield KedraExtractedDocumentItem(
            title=title,
            published_date=published_date,
            partition_date=partition_date,
            content=content,
            content_hash=content_hash,
            identifier=identifier,
            source=self.source,
            category=category,
            source_format="html",
            doc_url=response.url,
            landing_url=landing_url,
            raw_content=response.body,
            description=description,
        )

    def parse_pdf(
        self,
        response,
        title: str,
        identifier: str | None,
        published_date: str,
        partition_date: str,
        category: str,
        description: str | None,
        landing_url: str,
    ):
        """Extract judgment text directly from UKSC PDF decision files."""
        if self.scrape_mode == "ingestion":
            self.crawler.stats.inc_value("documents/pdf_ingested")
            yield KedraRawDocumentItem(
                title=title,
                published_date=published_date,
                partition_date=partition_date,
                identifier=identifier,
                source=self.source,
                category=category,
                source_format="pdf",
                doc_url=response.url,
                landing_url=landing_url,
                raw_content=response.body,
                description=description,
            )
            return

        try:
            reader = PdfReader(BytesIO(response.body))
            content = "\n\n".join(
                text
                for page in reader.pages
                if (text := (page.extract_text() or "").strip())
            )
        except Exception as exc:
            self.crawler.stats.inc_value("errors/pdf_parsing")
            record_body_metric(self, partition_date, category, "failed")
            log_structured(
                self.logger,
                logging.ERROR,
                "pdf_extraction_failed",
                "PDF parsing failed",
                exc_info=True,
                partition_date=partition_date,
                body=category,
                identifier=identifier,
                url=response.url,
                status_code=response.status,
                error_type=type(exc).__name__,
                reason=str(exc),
            )
            return

        if not content:
            self.crawler.stats.inc_value("errors/pdf_empty")
            record_body_metric(self, partition_date, category, "failed")
            return

        content_hash = hash_document(content)
        self.crawler.stats.inc_value("documents/pdf_extracted")
        yield KedraExtractedDocumentItem(
            title=title,
            published_date=published_date,
            partition_date=partition_date,
            content=content,
            content_hash=content_hash,
            identifier=identifier,
            source=self.source,
            category=category,
            source_format="pdf",
            doc_url=response.url,
            landing_url=landing_url,
            raw_content=response.body,
            description=description,
        )

    def handle_request_error(self, failure) -> None:
        record_request_error(self, failure)

    def handle_search_request_error(self, failure) -> None:
        record_request_error(self, failure, request_kind="search")

    def closed(self, reason: str) -> None:
        """Log structured crawl summary and write JSON reconciliation summary."""
        stats = self.crawler.stats
        expected = stats.get_value("documents/expected", 0)
        scraped = stats.get_value("item_scraped_count", 0)
        summary = {
            "reason": reason,
            "expected": expected,
            "scraped": scraped,
            "missing": max(expected - scraped, 0),
            "html_extracted": stats.get_value("documents/html_extracted", 0),
            "pdf_extracted": stats.get_value("documents/pdf_extracted", 0),
            "body_partitions": body_partition_summaries(self),
        }

        summary_path = self.settings.get("CRAWL_SUMMARY_PATH")
        if summary_path:
            try:
                write_crawl_summary(summary_path, summary)
            except (OSError, TypeError, ValueError):
                self.logger.exception("Could not write crawl summary to %s", summary_path)

        log_structured(self.logger, logging.INFO, "crawl_summary", "Crawl completed", **summary)

    def _parse_scraped_date(self, value: str, argument_name: str) -> date:
        for fmt in self.source_config.input_date_formats:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"{argument_name} invalid date format: {value}")

    @classmethod
    def _period_partitions(
        cls, start_date: date, end_date: date, partition_months: int
    ) -> Iterator[tuple[date, date]]:
        partition_start = start_date
        while partition_start <= end_date:
            month_start = date(partition_start.year, partition_start.month, 1)
            next_partition = cls._add_months(month_start, partition_months)
            partition_end = min(end_date, next_partition - timedelta(days=1))
            yield partition_start, partition_end
            partition_start = next_partition

    @staticmethod
    def _add_months(value: date, months: int) -> date:
        month_index = value.year * 12 + value.month - 1 + months
        return date(month_index // 12, month_index % 12 + 1, 1)

    @classmethod
    def _selector_text(cls, selector) -> str:
        if selector is None:
            return ""
        return " ".join(" ".join(selector.xpath(".//text()").getall()).split())

    def _normalise_published_date(self, value: str) -> str:
        cleaned_value = " ".join((value or "").split())
        for date_format in self.source_config.published_date_formats:
            try:
                return datetime.strptime(cleaned_value, date_format).date().isoformat()
            except ValueError:
                continue
        return cleaned_value

    @staticmethod
    def _is_pdf_url(url: str) -> bool:
        return urlsplit(url).path.lower().endswith(".pdf")