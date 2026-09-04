from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Mapping

from itemadapter import ItemAdapter
from scrapy import signals


_BODY_METRICS = ("found", "succeeded", "failed", "request_failures")


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


def record_body_metric(
    spider,
    partition_date: str | None,
    body: str | None,
    metric: str,
    count: int = 1,
) -> None:
    """Increment one body/month reconciliation metric."""
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
    """Enable JSON logs and observe item-level pipeline outcomes."""

    def __init__(self, crawler) -> None:
        self.crawler = crawler
        formatter = JsonLogFormatter()
        for handler in logging.getLogger().handlers:
            handler.setFormatter(formatter)

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
