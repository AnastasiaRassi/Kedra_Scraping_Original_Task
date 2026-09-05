from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
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

    if not isinstance(raw, dict):
        raise ValueError("Source registry root must be a JSON object")

    sources = raw.get("sources")
    if not isinstance(sources, dict) or not sources:
        raise ValueError("sources must be a non-empty JSON object")

    registry: dict[str, SourceRegistryEntry] = {}
    for raw_key, raw_entry in sources.items():
        if not isinstance(raw_key, str) or not raw_key.strip():
            raise ValueError("Every source key must be a non-empty string")
        source_key = raw_key.strip()
        context = f"sources.{source_key}"

        if not isinstance(raw_entry, dict):
            raise ValueError(f"{context} must be a JSON object")

        required: dict[str, str] = {}
        for field_name in ("spider", "source", "site_config_setting"):
            field_value = raw_entry.get(field_name)
            if not isinstance(field_value, str) or not field_value.strip():
                raise ValueError(
                    f"{context}.{field_name} must be a non-empty string"
                )
            required[field_name] = field_value.strip()

        raw_settings = raw_entry.get("spider_settings", {})
        if not isinstance(raw_settings, dict):
            raise ValueError(f"{context}.spider_settings must be a JSON object")

        spider_settings: dict[str, str] = {}
        for raw_name, raw_setting in raw_settings.items():
            if not isinstance(raw_name, str) or not raw_name.strip():
                raise ValueError(
                    f"{context}.spider_settings keys must be non-empty strings"
                )
            setting_name = raw_name.strip()
            if not isinstance(raw_setting, str) or not raw_setting.strip():
                raise ValueError(
                    f"{context}.spider_settings.{setting_name} "
                    "must be a non-empty string"
                )
            spider_settings[setting_name] = raw_setting.strip()

        site_config_setting = required["site_config_setting"]
        if site_config_setting not in spider_settings:
            raise ValueError(
                f"{context}.site_config_setting must name a key in "
                f"{context}.spider_settings"
            )
        site_config_path = spider_settings[site_config_setting]

        registry[source_key] = SourceRegistryEntry(
            key=source_key,
            spider=required["spider"],
            source=required["source"],
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

    if not isinstance(raw, dict):
        raise ValueError("Site config root must be a JSON object")

    selectors = raw.get("selectors")
    if not isinstance(selectors, dict):
        raise ValueError("Site config selectors must be a JSON object")

    html_content = selectors.get("html_content")
    if not isinstance(html_content, list) or not html_content:
        raise ValueError(
            "Site config selectors.html_content must be a non-empty list"
        )

    normalized: list[str] = []
    for index, selector in enumerate(html_content):
        if not isinstance(selector, str) or not selector.strip():
            raise ValueError(
                "Site config selectors.html_content"
                f"[{index}] must be a non-empty string"
            )
        normalized.append(selector.strip())

    return tuple(normalized)


def resolve_project_path(value: str) -> Path:
    """Resolve relative configuration paths from the repository root."""
    path = Path(value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path

