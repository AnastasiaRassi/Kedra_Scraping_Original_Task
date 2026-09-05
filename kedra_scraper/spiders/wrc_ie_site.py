from __future__ import annotations

import json
from dataclasses import dataclass

from kedra_scraper.source_registry import resolve_project_path


@dataclass(frozen=True)
class BodyCategoryConfig:
    """One WRC decision body and its search-form value."""

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

    if not isinstance(raw, dict):
        raise ValueError("WRC config root must be a JSON object")

    sections: dict[str, dict] = {}
    for section_name in ("form", "selectors", "date_formats", "search_query"):
        section = raw.get(section_name)
        if not isinstance(section, dict):
            raise ValueError(f"{section_name} must be a JSON object")
        sections[section_name] = section

    root_strings: dict[str, str] = {}
    for field_name in ("source", "search_url"):
        field_value = raw.get(field_name)
        if not isinstance(field_value, str) or not field_value.strip():
            raise ValueError(f"{field_name} must be a non-empty string")
        root_strings[field_name] = field_value.strip()

    raw_domains = raw.get("allowed_domains")
    if not isinstance(raw_domains, list) or not raw_domains:
        raise ValueError(
            "allowed_domains must be a non-empty list of strings"
        )
    allowed_domains: list[str] = []
    for index, domain in enumerate(raw_domains):
        if not isinstance(domain, str) or not domain.strip():
            raise ValueError(
                f"allowed_domains[{index}] must be a non-empty string"
            )
        allowed_domains.append(domain.strip())

    raw_categories = raw.get("body_categories")
    if not isinstance(raw_categories, list) or not raw_categories:
        raise ValueError("body_categories must be a non-empty list")

    body_categories: list[BodyCategoryConfig] = []
    for index, raw_category in enumerate(raw_categories):
        context = f"body_categories[{index}]"
        if not isinstance(raw_category, dict):
            raise ValueError(f"{context} must be a JSON object")

        values: dict[str, str] = {}
        for field_name in ("name", "form_field", "form_value"):
            field_value = raw_category.get(field_name)
            if not isinstance(field_value, str) or not field_value.strip():
                raise ValueError(
                    f"{context}.{field_name} must be a non-empty string"
                )
            values[field_name] = field_value.strip()

        body_categories.append(
            BodyCategoryConfig(
                name=values["name"],
                form_field=values["form_field"],
                form_value=values["form_value"],
            )
        )

    search_query: dict[str, str] = {}
    for raw_key, raw_value in sections["search_query"].items():
        if not isinstance(raw_key, str) or not raw_key.strip():
            raise ValueError("search_query keys must be non-empty strings")
        query_key = raw_key.strip()
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ValueError(
                f"search_query.{query_key} must be a non-empty string"
            )
        search_query[query_key] = raw_value.strip()

    form_values: dict[str, str] = {}
    for field_name in (
        "xpath",
        "start_date_field",
        "end_date_field",
        "submit_field",
    ):
        field_value = sections["form"].get(field_name)
        if not isinstance(field_value, str) or not field_value.strip():
            raise ValueError(
                f"form.{field_name} must be a non-empty string"
            )
        form_values[field_name] = field_value.strip()

    submit_value = sections["form"].get("submit_value", "")
    if not isinstance(submit_value, str):
        raise ValueError("form.submit_value must be a string")

    selector_values: dict[str, str] = {}
    for field_name in (
        "result_card",
        "document_link",
        "identifier",
        "published_date",
        "description",
        "next_page",
        "pdf_download",
    ):
        field_value = sections["selectors"].get(field_name)
        if not isinstance(field_value, str) or not field_value.strip():
            raise ValueError(
                f"selectors.{field_name} must be a non-empty string"
            )
        selector_values[field_name] = field_value.strip()

    raw_html_content = sections["selectors"].get("html_content")
    if not isinstance(raw_html_content, list) or not raw_html_content:
        raise ValueError(
            "selectors.html_content must be a non-empty list of strings"
        )
    html_content: list[str] = []
    for index, selector in enumerate(raw_html_content):
        if not isinstance(selector, str) or not selector.strip():
            raise ValueError(
                f"selectors.html_content[{index}] "
                "must be a non-empty string"
            )
        html_content.append(selector.strip())

    raw_input_formats = sections["date_formats"].get("input")
    if not isinstance(raw_input_formats, list) or not raw_input_formats:
        raise ValueError(
            "date_formats.input must be a non-empty list of strings"
        )
    input_formats: list[str] = []
    for index, date_format in enumerate(raw_input_formats):
        if not isinstance(date_format, str) or not date_format.strip():
            raise ValueError(
                f"date_formats.input[{index}] "
                "must be a non-empty string"
            )
        input_formats.append(date_format.strip())

    raw_published_formats = sections["date_formats"].get("published")
    if not isinstance(raw_published_formats, list) or not raw_published_formats:
        raise ValueError(
            "date_formats.published must be a non-empty list of strings"
        )
    published_formats: list[str] = []
    for index, date_format in enumerate(raw_published_formats):
        if not isinstance(date_format, str) or not date_format.strip():
            raise ValueError(
                f"date_formats.published[{index}] "
                "must be a non-empty string"
            )
        published_formats.append(date_format.strip())

    form_date_format = sections["date_formats"].get("form")
    if not isinstance(form_date_format, str) or not form_date_format.strip():
        raise ValueError("date_formats.form must be a non-empty string")

    return WRCSourceConfig(
        source=root_strings["source"],
        search_url=root_strings["search_url"],
        allowed_domains=tuple(allowed_domains),
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
            html_content=tuple(html_content),
        ),
        input_date_formats=tuple(input_formats),
        published_date_formats=tuple(published_formats),
        form_date_format=form_date_format.strip(),
    )

