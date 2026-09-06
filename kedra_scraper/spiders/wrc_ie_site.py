from __future__ import annotations

import json
from dataclasses import dataclass

from kedra_scraper.utils.data_validators import (
    mapping,
    non_empty_string,
    optional_string,
    resolve_project_path,
    string_dict,
    string_tuple,
    text,
)


@dataclass(frozen=True)
class BodyCategoryConfig:
    """One WRC decision body and its search form value."""

    name: str
    form_field: str
    form_value: str


@dataclass(frozen=True)
class FormConfig:
    """Fields required to submit the WRC search form."""

    xpath: str
    start_date_field: str
    end_date_field: str
    submit_field: str
    submit_value: str


@dataclass(frozen=True)
class SelectorConfig:
    """CSS selectors used to discover and extract WRC documents."""

    result_card: str
    document_link: str
    identifier: str
    published_date: str
    description: str
    next_page: str
    pdf_download: str
    html_content: tuple[str, ...]


@dataclass(frozen=True)
class WRCSourceConfig:
    """Validated runtime representation of the WRC site configuration."""

    source: str
    search_url: str
    allowed_domains: tuple[str, ...]
    search_query: dict[str, str]
    form: FormConfig
    body_categories: tuple[BodyCategoryConfig, ...]
    selectors: SelectorConfig
    input_date_formats: tuple[str, ...]
    published_date_formats: tuple[str, ...]
    form_date_format: str


def load_wrc_source_config(value: str) -> WRCSourceConfig:
    """Load and validate the source-specific WRC scraping configuration."""
    path = resolve_project_path(value)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"WRC config file does not exist: {path}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read WRC config file {path}: {exc}") from exc

    root = mapping(raw, "WRC config root")

    sections: dict[str, dict] = {}
    for section_name in ("form", "selectors", "date_formats", "search_query"):
        sections[section_name] = mapping(
            root.get(section_name),
            section_name,
        )

    source = text(root, "source", "WRC config")
    search_url = text(root, "search_url", "WRC config")
    allowed_domains = string_tuple(
        root.get("allowed_domains"),
        "allowed_domains",
    )

    raw_categories = root.get("body_categories")
    if not isinstance(raw_categories, list) or not raw_categories:
        raise ValueError("body_categories must be a non-empty list")

    body_categories: list[BodyCategoryConfig] = []
    for index, raw_category in enumerate(raw_categories):
        context = f"body_categories[{index}]"
        cat_dict = mapping(raw_category, context)
        body_categories.append(
            BodyCategoryConfig(
                name=text(cat_dict, "name", context),
                form_field=text(cat_dict, "form_field", context),
                form_value=text(cat_dict, "form_value", context),
            )
        )

    search_query = string_dict(sections["search_query"], "search_query")

    form_dict = sections["form"]
    form_values = {
        field: text(form_dict, field, "form")
        for field in (
            "xpath",
            "start_date_field",
            "end_date_field",
            "submit_field",
        )
    }
    submit_value = optional_string(form_dict.get("submit_value", ""), "form.submit_value")

    sel_dict = sections["selectors"]
    selector_values = {
        field: text(sel_dict, field, "selectors")
        for field in (
            "result_card",
            "document_link",
            "identifier",
            "published_date",
            "description",
            "next_page",
            "pdf_download",
        )
    }
    html_content = string_tuple(
        sel_dict.get("html_content"),
        "selectors.html_content",
    )

    date_dict = sections["date_formats"]
    input_formats = string_tuple(date_dict.get("input"), "date_formats.input")
    published_formats = string_tuple(
        date_dict.get("published"),
        "date_formats.published",
    )
    form_date_format = text(date_dict, "form", "date_formats")

    return WRCSourceConfig(
        source=source,
        search_url=search_url,
        allowed_domains=allowed_domains,
        search_query=search_query,
        form=FormConfig(
            xpath=form_values["xpath"],
            start_date_field=form_values["start_date_field"],
            end_date_field=form_values["end_date_field"],
            submit_field=form_values["submit_field"],
            submit_value=submit_value,
        ),
        body_categories=tuple(body_categories),
        selectors=SelectorConfig(
            result_card=selector_values["result_card"],
            document_link=selector_values["document_link"],
            identifier=selector_values["identifier"],
            published_date=selector_values["published_date"],
            description=selector_values["description"],
            next_page=selector_values["next_page"],
            pdf_download=selector_values["pdf_download"],
            html_content=html_content,
        ),
        input_date_formats=input_formats,
        published_date_formats=published_formats,
        form_date_format=form_date_format,
    )
