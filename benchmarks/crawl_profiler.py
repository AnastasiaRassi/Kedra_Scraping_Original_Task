from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from kedra_scraper.config import load_source_registry


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
ASSIGNMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    spider_args = parse_assignments(args.spider_arg, "spider argument")
    overrides = parse_assignments(args.setting, "setting")
    spider, source, source_settings = resolve_target(
        source=args.source,
        spider=args.spider,
        registry_path=args.registry,
    )

    destination = (
        args.output.expanduser()
        if args.output
        else _default_destination(source or spider)
    )
    if not destination.is_absolute():
        destination = PROJECT_ROOT / destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    structured_log_path = _profile_log_path(destination)

    settings = dict(source_settings)
    settings.update(overrides)
    settings.update(
        {
            "CRAWL_PROFILE_PATH": str(destination),
            "CRAWL_PROFILE_SOURCE": source or "",
            "LOG_LEVEL": args.log_level.upper(),
        }
    )
    settings.setdefault("STRUCTURED_LOG_PATH", str(structured_log_path))
    settings["PERSISTENCE_ENABLED"] = (
        "true" if args.with_persistence else "false"
    )
    if args.max_items is not None:
        settings["CLOSESPIDER_ITEMCOUNT"] = str(args.max_items)

    command = [sys.executable, "-m", "scrapy", "crawl", spider]
    for name, value in spider_args.items():
        command.extend(("-a", f"{name}={value}"))
    for name, value in settings.items():
        command.extend(("-s", f"{name}={value}"))

    print(f"Profiling spider={spider} source={source or 'direct'}")
    print(f"Structured log: {settings['STRUCTURED_LOG_PATH']}")
    completed = subprocess.run(command, cwd=PROJECT_ROOT, check=False)

    if destination.exists():
        report = json.loads(destination.read_text(encoding="utf-8"))
        print_profile(report, destination)
    else:
        print(
            "No profile was written. Check the Scrapy error above; the spider "
            "may have failed before it opened.",
            file=sys.stderr,
        )
        return completed.returncode or 1

    return completed.returncode


def resolve_target(
    *,
    source: str | None,
    spider: str | None,
    registry_path: str,
) -> tuple[str, str | None, dict[str, str]]:
    if spider:
        return spider, None, {}

    registry = load_source_registry(registry_path)
    try:
        entry = registry[source or ""]
    except KeyError as exc:
        available = ", ".join(sorted(registry))
        raise SystemExit(
            f"Unknown source {source!r}. Available sources: {available}"
        ) from exc
    return entry.spider, entry.key, dict(entry.spider_settings)


def parse_assignments(values: list[str], label: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for assignment in values:
        if "=" not in assignment:
            raise SystemExit(f"Invalid {label} {assignment!r}; use NAME=VALUE")
        name, value = assignment.split("=", 1)
        name = name.strip()
        if not ASSIGNMENT_NAME.fullmatch(name):
            raise SystemExit(f"Invalid {label} name: {name!r}")
        parsed[name] = value
    return parsed


def print_profile(report: dict, destination: Path) -> None:
    documents = report["documents"]
    latency = report["latency_seconds"]
    retries = report["retries"]
    failures = report["failures"]

    print("\nCrawl profile")
    print(f"  Report: {destination}")
    print(f"  Duration: {report['duration_seconds']:.3f} seconds")
    print(
        "  Documents: "
        f"expected={documents['expected']} scraped={documents['scraped']} "
        f"missing={documents['missing']} "
        f"unexplained={_display_count(documents['unexplained_missing'])}"
    )
    print(
        "  Throughput: "
        f"{documents['throughput_per_minute']:.3f} documents/minute"
    )
    print(
        "  Latency: "
        f"p50={_display_latency(latency['p50'])} "
        f"p95={_display_latency(latency['p95'])} "
        f"p99={_display_latency(latency['p99'])}"
    )
    print(
        "  Retries: "
        f"attempts={retries['attempts']} "
        f"recovered={retries['successful_responses_after_retry']} "
        f"exhausted={retries['exhausted']}"
    )
    print(
        "  Failures: "
        f"document_requests={failures['document_requests']} "
        f"search_requests={failures['search_requests']} "
        f"extraction={failures['extraction']}"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one Scrapy spider and write a crawl-performance report."
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--source",
        help="Source key from config/sources.json.",
    )
    target.add_argument("--spider", help="Scrapy spider name.")
    parser.add_argument(
        "-a",
        "--spider-arg",
        action="append",
        default=[],
        metavar="NAME=VALUE",
    )
    parser.add_argument(
        "-s",
        "--setting",
        action="append",
        default=[],
        metavar="NAME=VALUE",
    )
    parser.add_argument(
        "--registry",
        default=os.getenv("SOURCE_REGISTRY_PATH", "config/sources.json"),
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--max-items", type=_positive_integer)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--with-persistence",
        action="store_true",
        help="Include configured MongoDB and MinIO persistence in the run.",
    )
    return parser



def _profile_log_path(profile_path: Path) -> Path:
    root = Path(os.getenv("SCRAPY_LOG_DIR", "logs")).expanduser()
    if not root.is_absolute():
        root = PROJECT_ROOT / root
    return root / "profiles" / f"{profile_path.stem}.jsonl"

def _default_destination(label: str) -> Path:
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", label)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return PROJECT_ROOT / "reports" / "crawl_profiles" / (
        f"{safe_label}_{timestamp}.json"
    )


def _positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _display_latency(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.4f}s"


def _display_count(value: int | None) -> str:
    return "n/a (sampled crawl)" if value is None else str(value)


if __name__ == "__main__":
    raise SystemExit(main())
