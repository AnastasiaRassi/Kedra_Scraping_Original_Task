from dataclasses import dataclass, field
import yaml

@dataclass(frozen=True)
class UKSCSelectors:
    result_card: str = "table.decided-cases-table tbody tr, div.case-item"
    document_link: str = "td.case-title a::attr(href), a.judgment-pdf::attr(href)"
    identifier: str = "td.citation-number::text, span.neutral-citation::text"  # e.g., [2024] UKSC 5
    published_date: str = "td.judgment-date::text, span.date::text"
    description: str = "td.case-summary::text, div.summary::text"
    next_page: str = "a.pagination-next::attr(href)"
    pdf_download: str = "a[href$='.pdf']::attr(href)"
    html_content: list[str] = field(default_factory=lambda: ["div.judgment-body", "article.case-detail"])

@dataclass(frozen=True)
class UKSCSourceConfig:
    source: str = "uksc_gb"
    search_url: str = "https://www.supremecourt.uk/decided-cases/index.html"
    allowed_domains: tuple[str, ...] = ("supremecourt.uk", "www.supremecourt.uk")
    selectors: UKSCSelectors = field(default_factory=UKSCSelectors)
    input_date_formats: tuple[str, ...] = ("%Y-%m-%d", "%d/%m/%Y")
    published_date_formats: tuple[str, ...] = ("%d %B %Y", "%d/%m/%Y", "%Y-%m-%d")

def load_uksc_source_config(config_path: str) -> UKSCSourceConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    selectors_data = data.get("selectors", {})
    return UKSCSourceConfig(
        source=data.get("source", "uksc_gb"),
        search_url=data.get("search_url", "https://www.supremecourt.uk/decided-cases/index.html"),
        allowed_domains=tuple(data.get("allowed_domains", ["supremecourt.uk", "www.supremecourt.uk"])),
        selectors=UKSCSelectors(**selectors_data) if selectors_data else UKSCSelectors(),
    )