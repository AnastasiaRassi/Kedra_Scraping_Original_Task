from dataclasses import dataclass, field
import yaml

@dataclass(frozen=True)
class UKSCSelectors:
    results_container: str = "//*[@id='search-and-filters-cases']"
    result_card: str = "#search-and-filters-cases ul > li"
    document_link: str = "div.parsed-content a::attr(href)"
    title: str = "div.parsed-content a::text"
    identifier: str = ".//div[contains(@class, 'inline-flex')][contains(text(), '[')]/text()"
    case_reference: str = ".//div[contains(@class, 'inline-flex')][contains(text(), 'UKSC/')]/text()"
    description: str = "p.parsed-content::text"
    next_page: str = "a[aria-label='Next page']::attr(href)"
    # Not yet configured/verified against the live site; referenced by
    # sc_gb_spider.py so they must exist to avoid AttributeError.
    published_date: str = ""
    pdf_download: str = ""
    # Judgment body is a flat run of <p> siblings after the heading, not a
    # wrapper element.
    html_content: tuple[str, ...] = ()
    # Href to a same-domain "Judgment (HTML version)" page, present only on
    # some cases; older cases only link off-site to BAILII instead.
    judgment_details_link: str = ""

@dataclass(frozen=True)
class UKSCQueryParams:
    from_param: str = "doisd"
    to_param: str = "doied"
    date_format: str = "%Y-%m-%d"
    page_param: str = "p"
    default_params: dict[str, str] = field(default_factory=lambda: {"cs": "Judgment given"})

@dataclass(frozen=True)
class UKSCSourceConfig:
    source: str = "uksc_gb"
    search_url: str = "https://www.supremecourt.uk/cases/index.html"
    request_type: str = "GET"
    allowed_domains: tuple[str, ...] = ("supremecourt.uk", "www.supremecourt.uk")
    selectors: UKSCSelectors = field(default_factory=UKSCSelectors)
    query_params: UKSCQueryParams = field(default_factory=UKSCQueryParams)
    metadata_selectors: dict[str, str] = field(
        default_factory=lambda: {
            "linked_case_href": ".//div[contains(@class, 'mt-2')]//div[contains(@class, 'parsed-content')]/a/@href"
        }
    )
    input_date_formats: tuple[str, ...] = ("%Y-%m-%d", "%d/%m/%Y")
    published_date_formats: tuple[str, ...] = ("%d %B %Y", "%d/%m/%Y", "%Y-%m-%d")

def load_uksc_source_config(config_path: str) -> UKSCSourceConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    selectors_data = dict(data.get("selectors", {}))
    query_params_data = data.get("query_params", {})

    if "html_content" in selectors_data:
        selectors_data["html_content"] = tuple(selectors_data["html_content"])

    return UKSCSourceConfig(
        source=data.get("source", "uksc_gb"),
        search_url=data.get("search_url", "https://www.supremecourt.uk/cases/index.html"),
        request_type=data.get("request_type", "GET"),
        allowed_domains=tuple(data.get("allowed_domains", ["supremecourt.uk", "www.supremecourt.uk"])),
        selectors=UKSCSelectors(**selectors_data) if selectors_data else UKSCSelectors(),
        query_params=UKSCQueryParams(**query_params_data) if query_params_data else UKSCQueryParams(),
        metadata_selectors=data.get("metadata_selectors", {
            "linked_case_href": ".//div[contains(@class, 'mt-2')]//div[contains(@class, 'parsed-content')]/a/@href"
        }),
    )