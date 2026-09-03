from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class BodyCategoryConfig:
    name: str
    form_field: str
    form_value: str


@dataclass(frozen=True)
class FormConfig:
    xpath: str
    start_date_field: str
    end_date_field: str
    submit_field: str
    submit_value: str


@dataclass(frozen=True)
class SelectorConfig:
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


@dataclass(frozen=True)
class SourceRegistryEntry:
    key: str
    spider: str
    source: str
    spider_settings: dict[str, str]
    html_content_selectors: tuple[str, ...]


def load_source_registry(value: str) -> dict[str, SourceRegistryEntry]:
    """Load the source registry used by generic Dagster assets."""
    path = resolve_project_path(value)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Source registry does not exist: {path}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read source registry {path}: {exc}") from exc

    root = _mapping(raw, "root")
    sources_raw = _mapping(root.get("sources"), "sources")
    if not sources_raw:
        raise ValueError("sources must contain at least one configured source")

    registry: dict[str, SourceRegistryEntry] = {}
    for key, item in sources_raw.items():
        source_key = _non_empty_string(key, "source key")
        context = f"sources.{source_key}"
        source = _mapping(item, context)
        settings_raw = _mapping(
            source.get("spider_settings", {}),
            f"{context}.spider_settings",
        )
        spider_settings = {
            _non_empty_string(
                name,
                f"{context}.spider_settings key",
            ): _non_empty_string(
                setting,
                f"{context}.spider_settings.{name}",
            )
            for name, setting in settings_raw.items()
        }
        registry[source_key] = SourceRegistryEntry(
            key=source_key,
            spider=_text(source, "spider", context),
            source=_text(source, "source", context),
            spider_settings=spider_settings,
            html_content_selectors=_string_tuple(
                source.get("html_content_selectors"),
                f"{context}.html_content_selectors",
            ),
        )

    return registry


def resolve_project_path(value: str) -> Path:
    """Resolve config paths consistently from the repository root."""
    path = Path(value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def load_wrc_source_config(value: str) -> WRCSourceConfig:
    """Load and validate the source-specific WRC scraping configuration."""
    path = resolve_project_path(value)

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"WRC config file does not exist: {path}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read WRC config file {path}: {exc}") from exc

    root = _mapping(raw, "root")
    form_raw = _mapping(root.get("form"), "form")
    selectors_raw = _mapping(root.get("selectors"), "selectors")
    date_formats = _mapping(root.get("date_formats"), "date_formats")
    query_raw = _mapping(root.get("search_query"), "search_query")

    categories_raw = root.get("body_categories")
    if not isinstance(categories_raw, list) or not categories_raw:
        raise ValueError("body_categories must be a non-empty list")

    categories_list: list[BodyCategoryConfig] = []
    for index, item in enumerate(categories_raw):
        context = f"body_categories[{index}]"
        category = _mapping(item, context)
        categories_list.append(
            BodyCategoryConfig(
                name=_text(category, "name", context),
                form_field=_text(category, "form_field", context),
                form_value=_text(category, "form_value", context),
            )
        )
    categories = tuple(categories_list)

    search_query = {
        _non_empty_string(key, "search_query key"): _non_empty_string(
            item,
            f"search_query[{key!r}]",
        )
        for key, item in query_raw.items()
    }

    return WRCSourceConfig(
        source=_text(root, "source", "root"),
        search_url=_text(root, "search_url", "root"),
        allowed_domains=_string_tuple(
            root.get("allowed_domains"),
            "allowed_domains",
        ),
        search_query=search_query,
        form=FormConfig(
            xpath=_text(form_raw, "xpath", "form"),
            start_date_field=_text(form_raw, "start_date_field", "form"),
            end_date_field=_text(form_raw, "end_date_field", "form"),
            submit_field=_text(form_raw, "submit_field", "form"),
            submit_value=_optional_string(
                form_raw.get("submit_value", ""),
                "form.submit_value",
            ),
        ),
        body_categories=categories,
        selectors=SelectorConfig(
            result_card=_text(selectors_raw, "result_card", "selectors"),
            document_link=_text(
                selectors_raw,
                "document_link",
                "selectors",
            ),
            identifier=_text(selectors_raw, "identifier", "selectors"),
            published_date=_text(
                selectors_raw,
                "published_date",
                "selectors",
            ),
            description=_text(selectors_raw, "description", "selectors"),
            next_page=_text(selectors_raw, "next_page", "selectors"),
            pdf_download=_text(
                selectors_raw,
                "pdf_download",
                "selectors",
            ),
            html_content=_string_tuple(
                selectors_raw.get("html_content"),
                "selectors.html_content",
            ),
        ),
        input_date_formats=_string_tuple(
            date_formats.get("input"),
            "date_formats.input",
        ),
        published_date_formats=_string_tuple(
            date_formats.get("published"),
            "date_formats.published",
        ),
        form_date_format=_text(date_formats, "form", "date_formats"),
    )


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _text(mapping: Mapping[str, Any], key: str, context: str) -> str:
    return _non_empty_string(mapping.get(key), f"{context}.{key}")


def _non_empty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _optional_string(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} must be a non-empty list")
    return tuple(
        _non_empty_string(item, f"{name}[{index}]")
        for index, item in enumerate(value)
    )
