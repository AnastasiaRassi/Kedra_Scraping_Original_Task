from dataclasses import dataclass, field
from typing import Any, Literal

import scrapy

@dataclass(kw_only=True)
class KedraRawDocumentItem:
    title: str
    published_date: str
    partition_date: str
    identifier: str | None
    source: str
    category: str
    source_format: Literal["html", "pdf"]
    doc_url: str
    landing_url: str
    raw_content: bytes = field(repr=False)
    description: str | None = None
    source_metadata: dict[str, Any] = field(default_factory=dict)
    metadata = scrapy.Field()  # e.g. {"case_reference": "UKSC/2009/0136", "subject": "COURT PROCEDURE"}

@dataclass(kw_only=True)
class KedraExtractedDocumentItem(KedraRawDocumentItem):
    content: str
    content_hash: str