from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Iterator
from urllib.parse import urlsplit

import scrapy
from pypdf import PdfReader

from kedra_scraper.items import KedraScraperItem


class WRC_IE_Spider(scrapy.Spider):
    name = "WRC_IE"
    allowed_domains = ["workplacerelations.ie"]
    search_url = "https://www.workplacerelations.ie/en/search/"
    source = "https://www.workplacerelations.ie"

    # ASP.NET checkbox field names and values used by the Body filter.
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

        self.start_date = self._parse_input_date(start_date, "start_date")
        self.end_date = self._parse_input_date(end_date, "end_date")

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

        if not result_cards:
            self.logger.debug("No results found at %s", response.url)

        for card in result_cards:
            document_href = card.css("h2.title a::attr(href)").get()
            identifier = self._selector_text(card.css("h2.title a"))
            published_date = self._normalise_published_date(
                self._selector_text(card.css(".date"))
            )
            description = self._selector_text(card.css("p.description")) or None

            if not document_href or not identifier or not published_date:
                self.logger.warning(
                    "Skipping an incomplete search result on %s",
                    response.url,
                )
                continue

            document_url = response.urljoin(document_href)
            callback = (
                self.parse_pdf
                if self._is_pdf_url(document_url)
                else self.parse_document
            )

            yield scrapy.Request(
                url=document_url,
                callback=callback,
                cb_kwargs={
                    "identifier": identifier,
                    "published_date": published_date,
                    "partition_date": partition_date,
                    "category": category,
                    "description": description,
                },
            )

        next_page = response.css(
            "nav.pages a.next::attr(href), "
            "ul.pagination a.next::attr(href), "
            "a.next::attr(href)"
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
        identifier: str,
        published_date: str,
        partition_date: str,
        category: str,
        description: str | None,
    ):
        """Extract a case whose document is an HTML page."""
        content_type = response.headers.get(b"Content-Type", b"").lower()
        if b"application/pdf" in content_type or self._is_pdf_url(response.url):
            yield from self.parse_pdf(
                response,
                identifier=identifier,
                published_date=published_date,
                partition_date=partition_date,
                category=category,
                description=description,
            )
            return

        content_node = response.css("h1.page-title + div.content")
        if not content_node:
            content_node = response.css("div.col-sm-9 > div.content")

        content = self._selector_text(content_node)

        # Some case pages may embed or link to a PDF instead of containing text.
        if not content:
            pdf_href = response.xpath(
                "//a[contains(translate(@href, 'PDF', 'pdf'), '.pdf')]/@href"
                " | //iframe[contains(translate(@src, 'PDF', 'pdf'), '.pdf')]/@src"
                " | //embed[contains(translate(@src, 'PDF', 'pdf'), '.pdf')]/@src"
            ).get()

            if pdf_href:
                yield response.follow(
                    pdf_href,
                    callback=self.parse_pdf,
                    cb_kwargs={
                        "identifier": identifier,
                        "published_date": published_date,
                        "partition_date": partition_date,
                        "category": category,
                        "description": description,
                    },
                )
                return

            self.logger.warning("No document content found at %s", response.url)

        document_heading = self._clean_text(
            content_node.css("h1, h2, h3").xpath("string(.)").get()
        )
        title = document_heading or description or identifier

        yield KedraScraperItem(
            title=title,
            published_date=published_date,
            partition_date=partition_date,
            content=content,
            identifier=identifier,
            source=self.source,
            category=category,
            source_format="html",
            doc_url=response.url,
            description=description,
        )

    def parse_pdf(
        self,
        response,
        identifier: str,
        published_date: str,
        partition_date: str,
        category: str,
        description: str | None,
    ):
        """Extract text and metadata from a PDF document."""
        pdf_title = ""
        content = ""

        try:
            reader = PdfReader(BytesIO(response.body))
            content = "\n\n".join(
                text
                for page in reader.pages
                if (text := (page.extract_text() or "").strip())
            )

            if reader.metadata:
                pdf_title = self._clean_text(
                    getattr(reader.metadata, "title", None)
                )
        except Exception as exc:
            self.logger.error("Could not read PDF %s: %s", response.url, exc)

        if not content:
            self.logger.warning(
                "No extractable PDF text found at %s; it may be scanned",
                response.url,
            )

        yield KedraScraperItem(
            title=pdf_title or description or identifier,
            published_date=published_date,
            partition_date=partition_date,
            content=content,
            identifier=identifier,
            source=self.source,
            category=category,
            source_format="pdf",
            doc_url=response.url,
            description=description,
        )

    @staticmethod
    def _parse_input_date(value: str, argument_name: str) -> date:
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
