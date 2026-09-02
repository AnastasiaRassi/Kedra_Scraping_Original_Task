# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from dataclasses import dataclass



@dataclass
class KedraScraperItem:
    title: str 
    published_date: str
    partition_date: str 
    url: str
    content: str | None = None
    description: str | None = None
    identifiers: str | None = None