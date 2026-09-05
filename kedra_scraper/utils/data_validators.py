from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def resolve_project_path(value: str) -> Path:
    """Resolve config paths consistently from the repository root."""
    path = Path(value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def mapping(value: Any, name: str) -> Mapping[str, Any]:
    """Validate that a value is a JSON-style mapping/dict."""
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return value


def non_empty_string(value: Any, name: str) -> str:
    """Validate that a value is a non-empty string."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def optional_string(value: Any, name: str, default: str = "") -> str:
    """Validate an optional string field."""
    if value is None:
        return default
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def text(data: Mapping[str, Any], key: str, context: str) -> str:
    """Extract a required non-empty string from a mapping."""
    return non_empty_string(data.get(key), f"{context}.{key}")


def string_tuple(value: Any, name: str) -> tuple[str, ...]:
    """Validate that a value is a non-empty list of non-empty strings."""
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} must be a non-empty list of strings")
    return tuple(
        non_empty_string(item, f"{name}[{index}]")
        for index, item in enumerate(value)
    )


def string_dict(value: Any, name: str) -> dict[str, str]:
    """Validate that a value is a mapping of non-empty string keys to non-empty string values."""
    data = mapping(value, name)
    result: dict[str, str] = {}
    for raw_key, raw_val in data.items():
        key = non_empty_string(raw_key, f"{name} keys")
        val = non_empty_string(raw_val, f"{name}.{key}")
        result[key] = val
    return result