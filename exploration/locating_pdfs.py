from urllib.parse import urlsplit

import scrapy


class PdfFinderSpider(scrapy.Spider):
    name = "pdf_finder"
    allowed_domains = ["workplacerelations.ie"]

    start_urls = [
        "https://www.workplacerelations.ie/en/search/"
        "?decisions=1&q=Declan%20Holden%20V"
    ]

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    def parse(self, response):
        # Check every result on this page
        for href in response.css(
            "h2.title a::attr(href)"
        ).getall():
            url = response.urljoin(href)

            if self.is_pdf_url(url):
                yield {
                    "pdf_url": url,
                    "found_on": response.url,
                    "type": "direct_result",
                }
            else:
                # Open the HTML case and check for PDF links inside it
                yield response.follow(
                    url,
                    callback=self.check_case,
                )

        # Continue through all pagination pages
        next_page = response.css(
            "nav.pages a.next::attr(href)"
        ).get()

        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
            )

    def check_case(self, response):
        content_type = response.headers.get(
            "Content-Type", b""
        ).decode().lower()

        # The requested case itself returned a PDF
        if "application/pdf" in content_type:
            yield {
                "pdf_url": response.url,
                "found_on": response.url,
                "type": "pdf_response",
            }
            return

        # The HTML case contains links to PDFs
        pdf_links = response.xpath(
            '//a[contains('
            'translate(@href, "PDF", "pdf"), ".pdf"'
            ')]/@href'
        ).getall()

        for href in pdf_links:
            yield {
                "pdf_url": response.urljoin(href),
                "found_on": response.url,
                "type": "linked_from_html",
            }

    @staticmethod
    def is_pdf_url(url):
        return urlsplit(url).path.lower().endswith(".pdf")