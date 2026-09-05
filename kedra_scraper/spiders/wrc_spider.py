from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Iterator
from urllib.parse import urlencode, urlsplit

import scrapy
from pypdf import PdfReader

from kedra_scraper.source_registry import apply_source_settings
from kedra_scraper.spiders.wrc_ie_site import (
    WRCSourceConfig,
    load_wrc_source_config,
)
from kedra_scraper.items import (
    KedraExtractedDocumentItem,
    KedraRawDocumentItem,
)
from kedra_scraper.utils.structured_logging import (
    body_partition_summaries,
    log_structured,
    record_body_metric,
)
from kedra_scraper.utils import (
    handle_request_error as record_request_error,
    hash_document,
    write_crawl_summary,
)


class WRC_IE_Spider(scrapy.Spider):
    name = "WRC_IE"
    source_key = "wrc_ie"

    @classmethod
    def update_settings(cls, settings) -> None:
        """Load WRC overrides while preserving higher-priority CLI settings."""
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
        """Load source rules and runtime parameters before crawling."""
        config_path = settings.get("WRC_CONFIG_PATH")
        if not config_path:
            raise ValueError("WRC_CONFIG_PATH must not be empty")

        self.source_config: WRCSourceConfig = load_wrc_source_config(
            config_path
        )
        self.allowed_domains = list(self.source_config.allowed_domains)
        self.search_url = self.source_config.search_url
        self.source = self.source_config.source
        self.partition_months = settings.getint("SCRAPE_PARTITION_MONTHS")
        self.scrape_mode = settings.get("SCRAPE_MODE")

        if self.partition_months < 1:
            raise ValueError("SCRAPE_PARTITION_MONTHS must be at least 1")
        if self.scrape_mode not in {"full", "ingestion"}:
            raise ValueError("SCRAPE_MODE must be either 'full' or 'ingestion'")

        start_value = (
            self._start_date_argument or settings.get("SCRAPE_START_DATE")
        )
        end_value = self._end_date_argument or settings.get("SCRAPE_END_DATE")

        if not start_value or not end_value:
            raise ValueError(
                "Both dates are required. Pass -a start_date and -a end_date "
                "or set SCRAPE_START_DATE and SCRAPE_END_DATE."
            )

        self.start_date = self._parse_scraped_date(
            start_value,
            "start_date",
        )
        self.end_date = self._parse_scraped_date(end_value, "end_date")

        if self.start_date > self.end_date:
            raise ValueError("start_date must be earlier than or equal to end_date")

    async def start(self):
        """Load the ASP.NET search form before submitting its partitions."""
        query = urlencode(self.source_config.search_query)
        separator = "&" if "?" in self.search_url else "?" # since ? is already in the search_url and it appears once only, we need to use & to append the query parameters
        url = f"{self.search_url}{separator}{query}" if query else self.search_url
        yield scrapy.Request(
            url=url,
            callback=self.start_partition_searches, # after loading the search form, submit one search for each partition and category
            errback=self.handle_search_request_error,
        )

    def start_partition_searches(self, response):
        """Submit one POST search for every period and Body category. 
        Loops over the period partitions and the body categories, and 
        submits a search form for each combination."""

        for partition_start, partition_end in self._period_partitions(
            self.start_date,
            self.end_date,
            self.partition_months,
        ):
            partition_date = partition_start.isoformat()

            for category_config in self.source_config.body_categories:
                record_body_metric(
                    self,
                    partition_date,
                    category_config.name,
                    "found",
                    count=0,
                )
                form = self.source_config.form
                form_data = {
                    form.start_date_field: partition_start.strftime(
                        self.source_config.form_date_format
                    ),
                    form.end_date_field: partition_end.strftime(
                        self.source_config.form_date_format
                    ),
                    category_config.form_field: category_config.form_value,
                    form.submit_field: form.submit_value,
                }

                yield scrapy.FormRequest.from_response(
                    response,
                    formxpath=form.xpath,
                    formdata=form_data,
                    callback=self.parse, # once the search results page is loaded, parse the results and schedule document requests
                    errback=self.handle_search_request_error,
                    cb_kwargs={
                        "category": category_config.name,
                        "partition_date": partition_date,
                    },
                )

    def parse(self, response, category: str, partition_date: str):
        """Parse a single search results page, validate metadata, and schedule document requests.

            Iterates through result cards on the page, records metrics for reconciliation, 
            and validates mandatory fields.
            Incomplete cards are logged and skipped. Valid entries are dynamically 
            routed to either the PDF or standard document parser with full callback arguments.

            Yields:
                scrapy.Request: Follow-up requests for individual documents or pagination.
            """        
        selectors = self.source_config.selectors
        result_cards = response.css(selectors.result_card)
        self.crawler.stats.inc_value(
            "documents/expected",
            count=len(result_cards),
        )
        record_body_metric(self, partition_date, category, "found", count=len(result_cards))

        if not result_cards:
            self.logger.debug("No results found at %s", response.url)

        for card in result_cards:
            document_href = card.css(selectors.document_link).get()
            identifier = self._selector_text(
                card.css(selectors.identifier)
            ) or None
            published_date = self._normalise_published_date(
                self._selector_text(card.css(selectors.published_date))
            )
            description = self._selector_text(
                card.css(selectors.description)
            ) or None
            title = "_".join(
                value for value in (identifier, description) if value
            )

            if not document_href or not title or not published_date:
                self.crawler.stats.inc_value("documents/incomplete_result")
                record_body_metric(self, partition_date, category,"failed",)
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
                    reason=(
                        "Missing document link, title, or published date"
                    ),
                )
                continue

            self.crawler.stats.inc_value("documents/scheduled")
            document_url = response.urljoin(document_href)
            callback = ( # what occurs after the document URL is fetched, parse pdf or document(html) depends on the extension .pdf or .html
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
        """Extract a case whose document is an HTML page."""
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

        pdf_href = response.css(
            self.source_config.selectors.pdf_download
        ).get()
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

        try:
            content_node = None
            for selector in self.source_config.selectors.html_content:
                selected = response.css(selector)
                if selected:
                    content_node = selected
                    break
            content = self._selector_text(content_node)
        except Exception as exc:
            self.crawler.stats.inc_value("errors/html_parsing")
            record_body_metric(self, partition_date, category, "failed")
            log_structured(
                self.logger,
                logging.ERROR,
                "html_extraction_failed",
                "HTML parsing failed",
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
            self.crawler.stats.inc_value("errors/html_empty")
            record_body_metric(self, partition_date, category, "failed")
            log_structured(
                self.logger,
                logging.WARNING,
                "html_extraction_failed",
                "No HTML document content found",
                partition_date=partition_date,
                body=category,
                identifier=identifier,
                url=response.url,
                status_code=response.status,
                error_type="empty_html_content",
                reason="Configured selectors returned no document text",
            )
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
        """Extract text from a PDF document."""
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

        content = ""

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
            log_structured(
                self.logger,
                logging.WARNING,
                "pdf_extraction_failed",
                "No extractable PDF text found",
                partition_date=partition_date,
                body=category,
                identifier=identifier,
                url=response.url,
                status_code=response.status,
                error_type="empty_pdf_content",
                reason="The PDF may be scanned and require OCR",
            )
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
        """Delegate request-failure recording to the shared utility."""
        record_request_error(self, failure)

    def handle_search_request_error(self, failure) -> None:
        """Record a search or pagination request after retries are exhausted."""
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
        extraction_failed = sum(
            stats.get_value(key, 0)
            for key in (
                "errors/html_parsing",
                "errors/html_empty",
                "errors/pdf_parsing",
                "errors/pdf_empty",
            )
        )
        dropped = stats.get_value("item_dropped_count", 0)
        missing = max(expected - scraped, 0)
        explained_missing = (
            request_failed
            + incomplete_results
            + extraction_failed
            + dropped
        )
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
            "expected": expected,
            "scraped": scraped,
            "request_failed": request_failed,
            "search_request_failed": search_request_failed,
            "extraction_failed": extraction_failed,
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
            "html_extracted": stats.get_value(
                "documents/html_extracted",
                0,
            ),
            "pdf_extracted": stats.get_value(
                "documents/pdf_extracted",
                0,
            ),
            "html_ingested": stats.get_value(
                "documents/html_ingested",
                0,
            ),
            "pdf_ingested": stats.get_value(
                "documents/pdf_ingested",
                0,
            ),
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
            "minio_legacy_removed": stats.get_value(
                "persistence/minio_legacy_removed",
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

    def _parse_scraped_date(self, value: str, argument_name: str) -> date:
        for date_format in self.source_config.input_date_formats:
            try:
                return datetime.strptime(value, date_format).date()
            except ValueError:
                continue

        accepted = ", ".join(self.source_config.input_date_formats)
        raise ValueError(
            f"{argument_name} must match one of {accepted}; got {value!r}"
        )

    @classmethod
    def _period_partitions(
        cls,
        start_date: date,
        end_date: date,
        partition_months: int,
    ) -> Iterator[tuple[date, date]]:
        partition_start = start_date

        while partition_start <= end_date:
            month_start = date(
                partition_start.year,
                partition_start.month,
                1,
            )
            next_partition = cls._add_months(
                month_start,
                partition_months,
            )
            partition_end = min(
                end_date,
                next_partition - timedelta(days=1),
            )
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
        return cls._clean_text(" ".join(selector.xpath(".//text()").getall()))

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
    def _is_pdf_url(url: str) -> bool:
        return urlsplit(url).path.lower().endswith(".pdf")
