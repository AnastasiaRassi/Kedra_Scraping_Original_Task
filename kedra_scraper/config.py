from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parent.parent

# these data classes define the form of the config required per site


@dataclass(frozen=True)
class SourceRegistryEntry: # schema of characteristics of a source's spider, representing entries in sources.json
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
