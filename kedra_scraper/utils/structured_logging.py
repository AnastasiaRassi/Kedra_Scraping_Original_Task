from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Mapping

from itemadapter import ItemAdapter
from scrapy import signals


_BODY_METRICS = ("found", "succeeded", "failed", "request_failures")
_SAFE_PATH_COMPONENT = re.compile(r"[^A-Za-z0-9_.-]+")


class JsonLogFormatter(logging.Formatter):
    """Render each Python log record as one valid JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                tz=timezone.utc,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        event = getattr(record, "event", None)
        if event:
            payload["event"] = event

        fields = getattr(record, "structured_fields", None)
        if isinstance(fields, Mapping):
            payload.update(fields)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(
            payload,
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        )


def log_structured(
    logger,
    level: int,
    event: str,
    message: str,
    *,
    exc_info: bool = False,
    **fields: Any,
) -> None:
    """Emit a JSON-formatted event with queryable contextual fields."""
    logger.log(
        level,
        message,
        exc_info=exc_info,
        extra={
            "event": event,
            "structured_fields": fields,
        },
    )


def default_structured_log_path(
    directory: str | Path,
    spider_name: str,
    *,
    timestamp: datetime | None = None,
    process_id: int | None = None,
) -> Path:
    """Build a collision-resistant path for a standalone Scrapy crawl."""
    moment = timestamp or datetime.now(timezone.utc)
    safe_name = _SAFE_PATH_COMPONENT.sub("_", spider_name).strip("._") or "spider"
    filename = (
        f"{moment.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_"
        f"{process_id if process_id is not None else os.getpid()}.jsonl"
    )
    return Path(directory).expanduser() / safe_name / filename


def record_body_metric(
    spider,
    partition_date: str | None,
    body: str | None,
    metric: str,
    count: int = 1,
) -> None:
    """Increment one body/month reconciliation metric. Note: In data engineering and web scraping, 
    reconciliation ensures that what you expect to scrape matches what you actually process and store."""

    if metric not in _BODY_METRICS:
        raise ValueError(f"Unsupported body metric: {metric}")
    if not partition_date or not body:
        return

    counts = getattr(spider, "_body_partition_counts", None)
    if counts is None:
        counts = {}
        spider._body_partition_counts = counts

    key = (partition_date, body)
    values = counts.setdefault(
        key,
        {name: 0 for name in _BODY_METRICS},
    )
    values[metric] += count
    spider.crawler.stats.inc_value(
        f"body_partition/{partition_date}/{body}/{metric}",
        count=count,
    )


def body_partition_summaries(spider) -> list[dict[str, Any]]:
    """Return deterministic reconciliation rows for every body/month."""
    counts = getattr(spider, "_body_partition_counts", {})
    return [
        {
            "partition_date": partition_date,
            "body": body,
            **{metric: values.get(metric, 0) for metric in _BODY_METRICS},
        }
        for (partition_date, body), values in sorted(counts.items())
    ]


class StructuredLoggingExtension:
    """Write Scrapy events to the console and a rotating JSONL file."""

    def __init__(self, crawler) -> None:
        self.crawler = crawler
        self.formatter = JsonLogFormatter()
        self.file_handler: RotatingFileHandler | None = None
        self.log_path: Path | None = None

        for handler in logging.getLogger().handlers:
            handler.setFormatter(self.formatter)

        crawler.signals.connect(
            self.spider_opened,
            signal=signals.spider_opened,
        )
        crawler.signals.connect(
            self.spider_closed,
            signal=signals.spider_closed,
        )
        crawler.signals.connect(
            self.engine_stopped,
            signal=signals.engine_stopped,
        )
        crawler.signals.connect(
            self.item_scraped,
            signal=signals.item_scraped,
        )
        crawler.signals.connect(
            self.item_dropped,
            signal=signals.item_dropped,
        )

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def spider_opened(self, spider) -> None:
        configured_path = self.crawler.settings.get("STRUCTURED_LOG_PATH")
        self.log_path = (
            Path(configured_path).expanduser()
            if configured_path
            else default_structured_log_path(
                self.crawler.settings.get("STRUCTURED_LOG_DIR", "logs"),
                spider.name,
            )
        )
        if not self.log_path.is_absolute():
            self.log_path = Path.cwd() / self.log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        self.file_handler = RotatingFileHandler(
            self.log_path,
            maxBytes=self.crawler.settings.getint(
                "STRUCTURED_LOG_MAX_BYTES",
                25_000_000,
            ),
            backupCount=self.crawler.settings.getint(
                "STRUCTURED_LOG_BACKUP_COUNT",
                5,
            ),
            encoding="utf-8",
        )
        self.file_handler.setLevel(self.crawler.settings.get("LOG_LEVEL", "INFO"))
        self.file_handler.setFormatter(self.formatter)
        logging.getLogger().addHandler(self.file_handler)
        self.crawler.stats.set_value(
            "logging/structured_log_path",
            str(self.log_path),
        )
        log_structured(
            spider.logger,
            logging.INFO,
            "structured_log_started",
            "Structured crawl log opened",
            path=str(self.log_path),
        )

    def spider_closed(self, spider, reason: str) -> None:
        if self.file_handler is None:
            return
        log_structured(
            spider.logger,
            logging.INFO,
            "structured_log_closed",
            "Structured crawl log closing",
            path=str(self.log_path),
            reason=reason,
        )
        self.file_handler.flush()

    def engine_stopped(self) -> None:
        # Engine shutdown happens after all spider_closed receivers, so crawl
        # summaries emitted by other extensions are retained before detaching.
        if self.file_handler is None:
            return
        self.file_handler.flush()
        logging.getLogger().removeHandler(self.file_handler)
        self.file_handler.close()
        self.file_handler = None

    def item_scraped(self, item, response, spider) -> None:
        document = ItemAdapter(item)
        record_body_metric(
            spider,
            document.get("partition_date"),
            document.get("category"),
            "succeeded",
        )

    def item_dropped(self, item, response, exception, spider) -> None:
        document = ItemAdapter(item)
        partition_date = document.get("partition_date")
        body = document.get("category")
        url = (
            document.get("doc_url")
            or document.get("landing_url")
            or getattr(response, "url", None)
        )
        record_body_metric(
            spider,
            partition_date,
            body,
            "failed",
        )
        log_structured(
            spider.logger,
            logging.ERROR,
            "item_dropped",
            "Document was dropped by an item pipeline",
            partition_date=partition_date,
            body=body,
            identifier=document.get("identifier"),
            url=url,
            status_code=getattr(response, "status", None),
            error_type=type(exception).__name__,
            reason=str(exception),
        )
