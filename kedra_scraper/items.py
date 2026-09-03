from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class KedraRawDocumentItem:
    """Source metadata and original bytes produced by the ingestion task."""

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


@dataclass
class KedraScraperItem:
    """Fully scraped document used by direct full-mode crawls."""

    title: str
    published_date: str
    partition_date: str
    content: str
    content_hash: str
    identifier: str | None
    source: str
    category: str
    source_format: Literal["html", "pdf"]
    doc_url: str
    landing_url: str
    raw_content: bytes = field(repr=False)
    description: str | None = None
    source_metadata: dict[str, Any] = field(default_factory=dict)
