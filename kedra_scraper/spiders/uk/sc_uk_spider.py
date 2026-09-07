import scrapy


class ScUkSpider(scrapy.Spider):
    name = "sc_uk"
    allowed_domains = ["supremecourt.uk"]
    start_urls = ["https://supremecourt.uk"]

    def parse(self, response):
        pass
