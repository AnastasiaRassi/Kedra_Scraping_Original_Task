# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from dataclasses import dataclass
from typing import Literal


@dataclass
class KedraScraperItem:
    title: str
    published_date: str
    partition_date: str
    content: str
    identifier: str | None
    source: str
    category: str
    source_format: Literal["html", "pdf"]
    doc_url: str
    description: str | None = None
