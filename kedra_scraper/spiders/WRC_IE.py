import scrapy


class WRC_IE_Spider(scrapy.Spider):
    name = "WRC_IE"
    allowed_domains = ["workplacerelations.ie"]
    start_urls = ["https://www.workplacerelations.ie/en/search/"]

    def parse(self, response):
        pass
