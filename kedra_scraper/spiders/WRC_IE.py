from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Iterator
from urllib.parse import urlsplit

import scrapy
from pypdf import PdfReader

from kedra_scraper.items import KedraScraperItem
from kedra_scraper.utils import (
    handle_request_error as record_request_error,
    hash_document,
)


class WRC_IE_Spider(scrapy.Spider):
    name = "WRC_IE"
    allowed_domains = ["workplacerelations.ie"]
    search_url = "https://www.workplacerelations.ie/en/search/"
    source = "https://www.workplacerelations.ie"

    # this'll be hardcoded for now & fixed later when we focus on reproducability
    body_categories = {
        "Employment Appeals Tribunal": (
            "ctl00$ContentPlaceHolder_Main$CB2$CB2_0",
            "2",
        ),
        "Equality Tribunal": (
            "ctl00$ContentPlaceHolder_Main$CB2$CB2_1",
            "1",
        ),
        "Labour Court": (
            "ctl00$ContentPlaceHolder_Main$CB2$CB2_2",
            "3",
        ),
        "Workplace Relations Commission": (
            "ctl00$ContentPlaceHolder_Main$CB2$CB2_3",
            "15376",
        ),
    }

    def __init__(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if not start_date or not end_date:
            raise ValueError(
                "Both dates are required. Example: "
                "scrapy crawl WRC_IE -a start_date=01-01-2024 "
                "-a end_date=31-12-2024"
            )

        self.start_date = self._parse_scraped_date(start_date, "start_date")
        self.end_date = self._parse_scraped_date(end_date, "end_date")

        if self.start_date > self.end_date:
            raise ValueError("start_date must be earlier than or equal to end_date")

    async def start(self):
        """Load the ASP.NET search form before submitting its partitions."""
        yield scrapy.Request(
            url=f"{self.search_url}?advance=true&decisions=1",
            callback=self.start_partition_searches,
        )

        

    def start_partition_searches(self, response):
        """Submit one POST search for every month and Body category."""
        for partition_start, partition_end in self._monthly_partitions(
            self.start_date,
            self.end_date,
        ):
            partition_date = partition_start.isoformat()

            for category, (body_field, body_value) in self.body_categories.items():
                form_data = {
                    "ctl00$ContentPlaceHolder_Main$TextBox2": (
                        partition_start.strftime("%d/%m/%Y")
                    ),
                    "ctl00$ContentPlaceHolder_Main$TextBox3": (
                        partition_end.strftime("%d/%m/%Y")
                    ),
                    body_field: body_value,
                    "ctl00$ContentPlaceHolder_Main$refine_btn": "",
                }

                yield scrapy.FormRequest.from_response(
                    response,
                    formxpath="(//form)[1]",
                    formdata=form_data,
                    callback=self.parse,
                    cb_kwargs={
                        "category": category,
                        "partition_date": partition_date,
                    },
                )

    def parse(self, response, category: str, partition_date: str):
        """Read one results page, follow its documents, then follow pagination."""
        result_cards = response.css("li.each-item")
        self.crawler.stats.inc_value(
            "documents/expected",
            count=len(result_cards),
        )

        if not result_cards:
            self.logger.debug("No results found at %s", response.url)

        for card in result_cards:
            document_href = card.css("h2.title a::attr(href)").get()
            identifier = (
                self._selector_text(card.css("h2.title a")) or None
            )
            published_date = self._normalise_published_date(
                self._selector_text(card.css(".date"))
            )
            description = (
                self._selector_text(card.css("p.description")) or None
            )
            title = "_".join(
                value
                for value in (identifier, description)
                if value
            )

            if not document_href or not title or not published_date:
                self.crawler.stats.inc_value("documents/incomplete_result")
                self.logger.warning(
                    "Skipping an incomplete search result on %s",
                    response.url,
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
                }
            )
        next_page = response.css(
            "nav.pages li.current + li a::attr(href)"
        ).get()


        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
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

        # Older WRC case pages are HTML wrappers around a case PDF.
        # Scope this selector to the case download so site-wide PDFs
        # (for example, the Cookie Policy) are never mistaken for the document.
        pdf_href = response.css(
            "div.related-item a.download::attr(href)"
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

        try:
            content_node = response.css("h1.page-title + div.content")
            if not content_node:
                content_node = response.css("div.col-sm-9 > div.content")

            content = self._selector_text(content_node)
        except Exception:
            self.crawler.stats.inc_value("errors/html_parsing")
            self.logger.exception(
                "HTML parsing failed: identifier=%s url=%s",
                identifier,
                response.url,
            )
            return

        if not content:
            self.crawler.stats.inc_value("errors/html_empty")
            self.logger.warning(
                "No HTML document content found: identifier=%s url=%s",
                identifier,
                response.url,
            )
            return

        content_hash = self._hash_content(
            content,
            identifier=identifier,
            url=response.url,
        )
        if content_hash is None:
            return

        self.crawler.stats.inc_value("documents/html_extracted")
        yield KedraScraperItem(
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
        content = ""

        try:
            reader = PdfReader(BytesIO(response.body))
            content = "\n\n".join(
                text
                for page in reader.pages
                if (text := (page.extract_text() or "").strip())
            )
        except Exception:
            self.crawler.stats.inc_value("errors/pdf_parsing")
            self.logger.exception(
                "PDF parsing failed: identifier=%s url=%s",
                identifier,
                response.url,
            )
            return

        if not content:
            self.crawler.stats.inc_value("errors/pdf_empty")
            self.logger.warning(
                "No extractable PDF text found: identifier=%s url=%s; "
                "the document may be scanned",
                identifier,
                response.url,
            )
            return

        content_hash = self._hash_content(
            content,
            identifier=identifier,
            url=response.url,
        )
        if content_hash is None:
            return

        self.crawler.stats.inc_value("documents/pdf_extracted")
        yield KedraScraperItem(
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

    def _hash_content(
        self,
        content: str,
        identifier: str | None,
        url: str,
    ) -> str | None:
        """Hash extracted content while accounting for unexpected failures."""
        try:
            return hash_document(content)
        except (TypeError, ValueError, UnicodeError):
            self.crawler.stats.inc_value("errors/document_hashing")
            self.logger.exception(
                "Document hashing failed: identifier=%s url=%s",
                identifier,
                url,
            )
            return None

    def handle_request_error(self, failure) -> None:
        """Delegate request-failure recording to the shared utility."""
        record_request_error(self, failure)

    def closed(self, reason: str) -> None:
        """Log a reconciliation summary after all requests and items finish."""
        stats = self.crawler.stats

        expected = stats.get_value("documents/expected", 0)
        scraped = stats.get_value("item_scraped_count", 0)
        request_failed = stats.get_value("documents/request_failed", 0)
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
                "errors/document_hashing",
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

        self.logger.info(
            "CRAWL SUMMARY: reason=%s expected=%d scraped=%d "
            "request_failed=%d extraction_failed=%d "
            "incomplete_results=%d dropped=%d missing=%d unexplained=%d",
            reason,
            expected,
            scraped,
            request_failed,
            extraction_failed,
            incomplete_results,
            dropped,
            missing,
            unexplained_missing,
        )

        if missing:
            self.logger.warning(
                "Crawl finished with missing documents: expected=%d "
                "scraped=%d missing=%d explained=%d unexplained=%d",
                expected,
                scraped,
                missing,
                min(explained_missing, missing),
                unexplained_missing,
            )

    @staticmethod
    def _parse_scraped_date(value: str, argument_name: str) -> date:
        for date_format in ("%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, date_format).date()
            except ValueError:
                continue

        raise ValueError(
            f"{argument_name} must use DD-MM-YYYY or YYYY-MM-DD; got {value!r}"
        )

    @staticmethod
    def _monthly_partitions(
        start_date: date,
        end_date: date,
    ) -> Iterator[tuple[date, date]]:
        partition_start = start_date

        while partition_start <= end_date:
            if partition_start.month == 12:
                next_month = date(partition_start.year + 1, 1, 1)
            else:
                next_month = date(
                    partition_start.year,
                    partition_start.month + 1,
                    1,
                )

            partition_end = min(end_date, next_month - timedelta(days=1))
            yield partition_start, partition_end
            partition_start = next_month

    @classmethod
    def _selector_text(cls, selector) -> str:
        return cls._clean_text(" ".join(selector.xpath(".//text()").getall()))

    @staticmethod
    def _clean_text(value: str | None) -> str:
        return " ".join((value or "").split())

    @classmethod
    def _normalise_published_date(cls, value: str) -> str:
        cleaned_value = cls._clean_text(value)

        for date_format in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(cleaned_value, date_format).date().isoformat()
            except ValueError:
                continue

        return cleaned_value

    @staticmethod
    def _is_pdf_url(url: str) -> bool:
        return urlsplit(url).path.lower().endswith(".pdf")
