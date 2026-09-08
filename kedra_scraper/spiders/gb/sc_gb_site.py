from __future__ import annotations

import json
from dataclasses import dataclass

from kedra_scraper.utils.data_validators import (
    mapping,
    resolve_project_path,
    string_dict,
    string_tuple,
    text,
)

# Note! :
# the judgment_html_link selector yields the url that sends us to grab the text,
# which is to be parsed using the html_content key


@dataclass(frozen=True)
class UKSCSelectors:
    """XPath selectors used to read one UKSC case page."""

    case_id: str
    title: str
    case_status: str
    judgment_date: str
    judgment_pdf: str
    judgment_html_link: str
    html_content: tuple[str, ...]


@dataclass(frozen=True)
class UKSCSourceConfig:
    """Validated runtime representation of the UKSC site configuration."""

    source: str
    sitemap_url: str
    case_url_pattern: str
    allowed_domains: tuple[str, ...]
    selectors: UKSCSelectors
    metadata_selectors: dict[str, str]
    metadata_list_selectors: dict[str, str]
    published_date_formats: tuple[str, ...]


def load_uksc_source_config(value: str) -> UKSCSourceConfig:
    """Load and validate the source-specific UKSC scraping configuration."""
    path = resolve_project_path(value)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"UKSC config file does not exist: {path}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read UKSC config file {path}: {exc}") from exc

    root = mapping(raw, "UKSC config root")
    sel_dict = mapping(root.get("selectors"), "selectors")
    date_dict = mapping(root.get("date_formats"), "date_formats")

    selector_values = {
        field: text(sel_dict, field, "selectors")
        for field in (
            "case_id",
            "title",
            "case_status",
            "judgment_date",
            "judgment_pdf",
            "judgment_html_link",
        )
    }

    return UKSCSourceConfig(
        source=text(root, "source", "UKSC config"),
        sitemap_url=text(root, "sitemap_url", "UKSC config"),
        case_url_pattern=text(root, "case_url_pattern", "UKSC config"),
        allowed_domains=string_tuple(
            root.get("allowed_domains"),
            "allowed_domains",
        ),
        selectors=UKSCSelectors(
            **selector_values,
            html_content=string_tuple(
                sel_dict.get("html_content"),
                "selectors.html_content",
            ),
        ),
        metadata_selectors=string_dict(
            root.get("metadata_selectors"),
            "metadata_selectors",
        ),
        metadata_list_selectors=string_dict(
            root.get("metadata_list_selectors"),
            "metadata_list_selectors",
        ),
        published_date_formats=string_tuple(
            date_dict.get("published"),
            "date_formats.published",
        ),
    )
