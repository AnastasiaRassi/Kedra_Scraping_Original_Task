from dataclasses import dataclass, field
from typing import Any, Literal

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
    # e.g. {"case_reference": "UKSC/2009/0136", "subject": "COURT PROCEDURE"}
    source_metadata: dict[str, Any] = field(default_factory=dict)