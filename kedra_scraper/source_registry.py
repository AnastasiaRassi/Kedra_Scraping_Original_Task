from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE_REGISTRY_PATH = "config/source_registry.json"


@dataclass(frozen=True)
class SourceRegistryEntry: 
    # Each new source gets an entry in the source registry, 
    # which is used to configure the generic Dagster assets.
    key: str
    spider: str
    source: str
    spider_settings: dict[str, str]
    site_config_path: str
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

    root = mapping(raw, "root")
    sources_raw = mapping(root.get("sources"), "sources")
    if not sources_raw:
        raise ValueError("sources must contain at least one configured source")

    registry: dict[str, SourceRegistryEntry] = {}
    for key, item in sources_raw.items():
        source_key = non_empty_string(key, "source key")
        context = f"sources.{source_key}"
        source = mapping(item, context)
        settings_raw = mapping(
            source.get("spider_settings", {}),
            f"{context}.spider_settings",
        )
        spider_settings = {
            non_empty_string(
                name,
                f"{context}.spider_settings key",
            ): non_empty_string(
                setting,
                f"{context}.spider_settings.{name}",
            )
            for name, setting in settings_raw.items()
        }
        site_config_setting = text(
            source,
            "site_config_setting",
            context,
        )
        try:
            site_config_path = spider_settings[site_config_setting]
        except KeyError as exc:
            raise ValueError(
                f"{context}.site_config_setting must name a key in "
                f"{context}.spider_settings"
            ) from exc

        registry[source_key] = SourceRegistryEntry(
            key=source_key,
            spider=text(source, "spider", context),
            source=text(source, "source", context),
            spider_settings=spider_settings,
            site_config_path=site_config_path,
            html_content_selectors=load_html_content_selectors(
                site_config_path
            ),
        )

    return registry


def apply_source_settings(settings: Any, source_key: str) -> None:
    """Apply a source's registry overrides at Scrapy's spider priority."""
    registry_path = settings.get("SOURCE_REGISTRY_PATH") or DEFAULT_SOURCE_REGISTRY_PATH
    registry = load_source_registry(str(registry_path))
    try:
        source = registry[source_key]
    except KeyError as exc:
        available = ", ".join(sorted(registry))
        raise ValueError(
            f"Unknown source {source_key!r}. Available sources: {available}"
        ) from exc

    # These measured per-site values override conservative project defaults.
    # Explicit command-line -s settings remain higher priority.
    for name, value in source.spider_settings.items():
        settings.set(name, value, priority="spider")


def load_html_content_selectors(value: str) -> tuple[str, ...]:
    """ Load generic extraction selectors from a site's own config file.(value) """
    
    path = resolve_project_path(value)
    try:
        raw_site_cfg = json.loads(path.read_text(encoding="utf-8"))

    except FileNotFoundError as exc:
        raise ValueError(f"Site config does not exist: {path}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read site config {path}: {exc}") from exc

    site_cfg_root = mapping(raw_site_cfg, "site config root") 
    selectors = mapping(site_cfg_root.get("selectors"), "site config selectors")
    return string_tuple(
        selectors.get("html_content"),
        "site config selectors.html_content",
    )


def resolve_project_path(value: str) -> Path:
    """Resolve config paths consistently from the repository root."""
    path = Path(value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def mapping(value: Any, name: str) -> Mapping[str, Any]:
    # check the value is a mapping
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def text(mapping: Mapping[str, Any], key: str, context: str) -> str:
    """Extract a non-empty string from a mapping, with context for error messages."""
    return non_empty_string(mapping.get(key), f"{context}.{key}")


def non_empty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def optional_string(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} must be a non-empty list")
    return tuple(
        non_empty_string(item, f"{name}[{index}]")
        for index, item in enumerate(value)
    )
