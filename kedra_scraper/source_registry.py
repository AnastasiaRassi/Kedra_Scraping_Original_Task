from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from kedra_scraper.utils.data_validators import (
    mapping,
    non_empty_string,
    resolve_project_path,
    string_dict,
    string_tuple,
    text,
)

DEFAULT_SOURCE_REGISTRY_PATH = "config/source_registry.json"


@dataclass(frozen=True)
class SourceRegistryEntry:
    """Validated runtime configuration for one registered source."""

    key: str
    spider: str
    source: str
    spider_settings: dict[str, str]
    site_config_path: str
    html_content_selectors: tuple[str, ...]


def load_source_registry(value: str) -> dict[str, SourceRegistryEntry]:
    """Load and validate the source registry used by generic Dagster assets."""
    path = resolve_project_path(value)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Source registry does not exist: {path}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read source registry {path}: {exc}") from exc

    root = mapping(raw, "Source registry root")
    raw_sources = mapping(root.get("sources"), "sources")
    if not raw_sources:
        raise ValueError("sources must be a non-empty JSON object")

    registry: dict[str, SourceRegistryEntry] = {}
    for raw_key, raw_entry in raw_sources.items():
        source_key = non_empty_string(raw_key, "Every source key")
        context = f"sources.{source_key}"
        entry_dict = mapping(raw_entry, context)

        spider = text(entry_dict, "spider", context)
        source_url = text(entry_dict, "source", context)
        site_config_setting = text(entry_dict, "site_config_setting", context)

        spider_settings = string_dict(
            entry_dict.get("spider_settings", {}),
            f"{context}.spider_settings",
        )

        if site_config_setting not in spider_settings:
            raise ValueError(
                f"{context}.site_config_setting must name a key in "
                f"{context}.spider_settings"
            )
        site_config_path = spider_settings[site_config_setting]

        registry[source_key] = SourceRegistryEntry(
            key=source_key,
            spider=spider,
            source=source_url,
            spider_settings=spider_settings,
            site_config_path=site_config_path,
            html_content_selectors=load_html_content_selectors(
                site_config_path
            ),
        )

    return registry


def apply_source_settings(settings: Any, source_key: str) -> None:
    """Apply a source's registry overrides at Scrapy's spider priority."""
    registry_path = (
        settings.get("SOURCE_REGISTRY_PATH")
        or DEFAULT_SOURCE_REGISTRY_PATH
    )
    registry = load_source_registry(str(registry_path))
    try:
        source = registry[source_key]
    except KeyError as exc:
        available = ", ".join(sorted(registry))
        raise ValueError(
            f"Unknown source {source_key!r}. Available sources: {available}"
        ) from exc

    for name, value in source.spider_settings.items():
        settings.set(name, value, priority="spider")


def load_html_content_selectors(value: str) -> tuple[str, ...]:
    """Load HTML extraction selectors from a source's site configuration."""
    path = resolve_project_path(value)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Site config does not exist: {path}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read site config {path}: {exc}") from exc

    root = mapping(raw, "Site config root")
    selectors = mapping(root.get("selectors"), "Site config selectors")
    html_content = selectors.get("html_content")
    return string_tuple(html_content, "Site config selectors.html_content")
