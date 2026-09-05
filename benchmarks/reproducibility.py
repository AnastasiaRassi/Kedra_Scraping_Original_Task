from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


SENSITIVE_NAME = re.compile(
    r"(?:password|passwd|secret|token|credential|api[_-]?key|access[_-]?key|uri|dsn)",
    re.IGNORECASE,
)
DEPENDENCIES = (
    "Scrapy",
    "pypdf",
    "pymongo",
    "python-dotenv",
    "minio",
    "dagster",
    "dagster-webserver",
    "dagster-graphql",
    "Twisted",
    "lxml",
)


def capture_reproducibility_metadata(
    *,
    project_root: Path,
    spider: str,
    source: str | None,
    spider_args: Mapping[str, str],
    setting_overrides: Mapping[str, str],
    config_paths: Iterable[str | Path],
    with_persistence: bool,
    max_items: int | None,
    log_level: str,
) -> dict[str, Any]:
    """Capture the code, runtime, inputs, and configs behind one benchmark."""
    configurations = configuration_fingerprints(project_root, config_paths)
    requirements_path = project_root / "requirements.txt"
    installed_distributions = installed_dependency_versions()

    return {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "invocation": {
            "spider": spider,
            "source": source,
            "spider_arguments": redact_mapping(spider_args),
            "setting_overrides": redact_mapping(setting_overrides),
            "with_persistence": with_persistence,
            "max_items": max_items,
            "log_level": log_level,
        },
        "git": git_state(project_root),
        "runtime": {
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "python_executable": sys.executable,
            "python_build": platform.python_build(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
            "direct_dependencies": dependency_versions(),
            "installed_distributions": installed_distributions,
            "installed_distributions_sha256": hashlib.sha256(
                json.dumps(
                    installed_distributions,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest(),
        },
        "source_configuration": {
            "files": configurations,
            "combined_sha256": combined_fingerprint(configurations),
        },
        "requirements": (
            file_fingerprint(project_root, requirements_path)
            if requirements_path.is_file()
            else None
        ),
        "response_replay": {
            "available": False,
            "reason": (
                "Live HTTP responses are not archived by the profiler. "
                "The experiment configuration is reproducible, but exact "
                "response-level replay requires a response archive."
            ),
        },
    }


def redact_mapping(values: Mapping[str, str]) -> dict[str, Any]:
    """Retain every input name without writing credentials into reports."""
    result: dict[str, Any] = {}
    for name, value in sorted(values.items()):
        if SENSITIVE_NAME.search(name):
            result[name] = {
                "redacted": True,
                "value_sha256": hashlib.sha256(value.encode("utf-8")).hexdigest(),
            }
        else:
            result[name] = value
    return result


def configuration_fingerprints(
    project_root: Path,
    paths: Iterable[str | Path],
) -> list[dict[str, Any]]:
    """Hash each unique configuration file used by the crawl."""
    resolved: dict[str, Path] = {}
    for value in paths:
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = project_root / path
        path = path.resolve()
        resolved[str(path)] = path

    return [
        file_fingerprint(project_root, path)
        for path in sorted(resolved.values(), key=lambda item: str(item))
    ]


def file_fingerprint(project_root: Path, path: Path) -> dict[str, Any]:
    """Describe a file without embedding its potentially sensitive contents."""
    try:
        display_path = str(path.resolve().relative_to(project_root.resolve()))
    except ValueError:
        display_path = str(path.resolve())

    if not path.is_file():
        return {
            "path": display_path,
            "exists": False,
            "sha256": None,
            "size_bytes": None,
        }

    content = path.read_bytes()
    return {
        "path": display_path,
        "exists": True,
        "sha256": hashlib.sha256(content).hexdigest(),
        "size_bytes": len(content),
    }


def combined_fingerprint(files: list[dict[str, Any]]) -> str:
    """Create one stable digest for the complete source configuration."""
    canonical = json.dumps(
        files,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def dependency_versions() -> dict[str, str | None]:
    """Return exact versions for the project's direct runtime dependencies."""
    versions: dict[str, str | None] = {}
    for distribution in DEPENDENCIES:
        try:
            versions[distribution] = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            versions[distribution] = None
    return versions


def installed_dependency_versions() -> dict[str, str]:
    """Snapshot every installed distribution, including transitive packages."""
    versions: dict[str, str] = {}
    for distribution in importlib.metadata.distributions():
        name = distribution.metadata.get("Name")
        if name:
            versions[name] = distribution.version
    return dict(sorted(versions.items(), key=lambda item: item[0].lower()))


def git_state(project_root: Path) -> dict[str, Any]:
    """Capture the tracked revision and whether local code differs from it."""
    commit = _git(project_root, "rev-parse", "HEAD")
    if commit is None:
        return {
            "available": False,
            "commit_sha": None,
            "branch": None,
            "dirty": None,
            "status_sha256": None,
            "tracked_diff_sha256": None,
        }

    branch = _git(project_root, "rev-parse", "--abbrev-ref", "HEAD")
    status = _git(
        project_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    tracked_diff = _git(project_root, "diff", "--binary", "HEAD")
    status_text = status or ""
    diff_text = tracked_diff or ""

    return {
        "available": True,
        "commit_sha": commit.strip(),
        "branch": branch.strip() if branch else None,
        "dirty": bool(status_text.strip()),
        "status_sha256": hashlib.sha256(
            status_text.encode("utf-8")
        ).hexdigest(),
        "tracked_diff_sha256": hashlib.sha256(
            diff_text.encode("utf-8")
        ).hexdigest(),
    }


def _git(project_root: Path, *arguments: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", *arguments],
            cwd=project_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout if result.returncode == 0 else None
