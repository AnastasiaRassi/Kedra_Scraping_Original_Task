from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from io import BytesIO
from time import sleep
from typing import TypeVar

from itemadapter import ItemAdapter
from minio import Minio
from minio.error import S3Error
from pymongo import ASCENDING, MongoClient
from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    PyMongoError,
    ServerSelectionTimeoutError,
)
from scrapy.exceptions import DropItem
from urllib3.exceptions import HTTPError as Urllib3HTTPError

from kedra_scraper.utils.structured_logging import log_structured
from kedra_scraper.utils import hash_document


T = TypeVar("T")

_RETRYABLE_S3_ERROR_CODES = {
    "InternalError",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "SlowDown",
    "Throttling",
    "TooManyRequests",
    "XMinioServerNotInitialized",
    "XMinioServerNotReady",
}
_RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}
_RETRYABLE_MONGODB_LABELS = {
    "RetryableWriteError",
    "TransientTransactionError",
}


def _is_transient_storage_error(exc: Exception) -> bool:
    """Return whether repeating a storage operation can reasonably recover."""
    if isinstance(exc, S3Error):
        status = getattr(getattr(exc, "response", None), "status", None)
        return (
            exc.code in _RETRYABLE_S3_ERROR_CODES
            or status in _RETRYABLE_HTTP_STATUSES
        )

    if isinstance(
        exc,
        (
            AutoReconnect,
            ConnectionFailure,
            NetworkTimeout,
            ServerSelectionTimeoutError,
        ),
    ):
        return True

    if isinstance(exc, PyMongoError):
        return any(
            exc.has_error_label(label)
            for label in _RETRYABLE_MONGODB_LABELS
        )

    return isinstance(
        exc,
        (ConnectionError, TimeoutError, OSError, Urllib3HTTPError),
    )


def _retry_with_exponential_backoff(
    operation: Callable[[], T],
    *,
    retry_times: int,
    base_delay: float,
    max_delay: float,
    should_retry: Callable[[Exception], bool],
    on_retry: Callable[[int, float, Exception], None] | None = None,
    sleeper: Callable[[float], None] = sleep,
) -> T:
    """Retry one synchronous operation using a bounded exponential delay."""
    if retry_times < 0:
        raise ValueError("retry_times must be non-negative")
    if base_delay < 0 or max_delay < 0:
        raise ValueError("retry delays must be non-negative")

    retries_used = 0
    while True:
        try:
            return operation()
        except Exception as exc:
            if retries_used >= retry_times or not should_retry(exc):
                raise

            retries_used += 1
            delay = min(base_delay * (2 ** (retries_used - 1)), max_delay)
            if on_retry is not None:
                on_retry(retries_used, delay, exc)
            sleeper(delay)


def _run_storage_operation(
    operation: Callable[[], T],
    *,
    spider,
    operation_name: str,
    retry_times: int,
    base_delay: float,
    max_delay: float,
) -> T:
    """Run and report a retry-safe MinIO or MongoDB operation."""
    retries_used = 0

    def record_retry(
        retry_number: int,
        delay: float,
        exc: Exception,
    ) -> None:
        nonlocal retries_used
        retries_used = retry_number
        spider.crawler.stats.inc_value("persistence/retry_attempts")
        spider.crawler.stats.inc_value(
            f"persistence/retry_attempts/{operation_name}"
        )
        log_structured(
            spider.logger,
            logging.WARNING,
            "persistence_retry",
            "Transient persistence operation failed; retrying",
            operation=operation_name,
            retry_number=retry_number,
            retry_limit=retry_times,
            delay_seconds=delay,
            error_type=type(exc).__name__,
            reason=str(exc),
        )

    try:
        result = _retry_with_exponential_backoff(
            operation,
            retry_times=retry_times,
            base_delay=base_delay,
            max_delay=max_delay,
            should_retry=_is_transient_storage_error,
            on_retry=record_retry,
        )
    except Exception as exc:
        if _is_transient_storage_error(exc):
            spider.crawler.stats.inc_value("persistence/retry_exhausted")
            spider.crawler.stats.inc_value(
                f"persistence/retry_exhausted/{operation_name}"
            )
        raise

    if retries_used:
        spider.crawler.stats.inc_value("persistence/retry_recovered")
        spider.crawler.stats.inc_value(
            f"persistence/retry_recovered/{operation_name}"
        )
        log_structured(
            spider.logger,
            logging.INFO,
            "persistence_retry_recovered",
            "Persistence operation recovered after retry",
            operation=operation_name,
            retries_used=retries_used,
        )
    return result


def _build_record_key(document: dict) -> str:
    """Build a stable identity independent of nullable source identifiers."""
    source = document.get("source")
    landing_url = document.get("landing_url")

    if not isinstance(source, str) or not source.strip():
        raise ValueError("source is required to build record_key")
    if not isinstance(landing_url, str) or not landing_url.strip():
        raise ValueError("landing_url is required to build record_key")

    identity = f"{source.rstrip('/')}|{landing_url.strip()}"
    return hash_document(identity)


def _build_blob_object_name(
    record_key: str,
    prefix: str,
    source_format: str,
) -> str:
    """Build a flat MinIO filename for one logical source document."""
    if source_format not in {"html", "pdf"}:
        raise ValueError("source_format must be either 'html' or 'pdf'")
    object_name = f"{record_key}.{source_format}"
    return f"{prefix}/{object_name}" if prefix else object_name


def _build_legacy_blob_object_name(record_key: str, prefix: str) -> str:
    """Return the superseded key that MinIO displays as a hash folder."""
    object_name = f"{record_key}/current"
    return f"{prefix}/{object_name}" if prefix else object_name


def _stat_object_or_none(client: Minio, bucket: str, object_name: str):
    try:
        return client.stat_object(bucket, object_name)
    except S3Error as exc:
        if exc.code not in {"NoSuchKey", "NoSuchObject"}:
            raise
        return None


def _remove_object_if_present(
    client: Minio,
    bucket: str,
    object_name: str,
) -> bool:
    """Remove an object if present and report whether it was found."""
    if _stat_object_or_none(client, bucket, object_name) is None:
        return False
    client.remove_object(bucket, object_name)
    return True


def _metadata_blob_hash(stat_result) -> str | None:
    if stat_result is None:
        return None
    return (
        stat_result.metadata.get("x-amz-meta-blob-hash")
        or stat_result.metadata.get("X-Amz-Meta-Blob-Hash")
        or stat_result.metadata.get("blob-hash")
    )


class MinioPipeline:
    """Upsert one current raw object per logical source document.

    ``record_key`` identifies the document from its source and landing URL, so
    it remains stable when that document's bytes change. The content SHA-256 is
    used only to detect whether those bytes changed. It is deliberately not
    included in the MinIO key: a hash-based key would create another object
    for every replacement instead of overwriting the older object, retaining
    historical blobs that this latest-state storage requirement does not need.
    Objects are stored flat as ``{record_key}.pdf`` or ``{record_key}.html``;
    the superseded ``{record_key}/current`` object is removed only after its
    flat replacement is safely stored. Retrying a transient upload is safe
    because every attempt targets the same stable filename with the same bytes.
    """

    def __init__(
        self,
        enabled: bool,
        endpoint: str,
        access_key: str | None,
        secret_key: str | None,
        secure: bool,
        bucket: str,
        prefix: str,
        retry_times: int = 2,
        retry_base_delay: float = 0.5,
        retry_max_delay: float = 5.0,
    ):
        self.enabled = enabled
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.retry_times = retry_times
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.client: Minio | None = None

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            enabled=settings.getbool("PERSISTENCE_ENABLED"),
            endpoint=settings.get("MINIO_ENDPOINT"),
            access_key=settings.get("MINIO_ACCESS_KEY"),
            secret_key=settings.get("MINIO_SECRET_KEY"),
            secure=settings.getbool("MINIO_SECURE"),
            bucket=settings.get("MINIO_BUCKET"),
            prefix=settings.get("MINIO_PREFIX"),
            retry_times=settings.getint("PERSISTENCE_RETRY_TIMES"),
            retry_base_delay=settings.getfloat(
                "PERSISTENCE_RETRY_BASE_DELAY_SECONDS"
            ),
            retry_max_delay=settings.getfloat(
                "PERSISTENCE_RETRY_MAX_DELAY_SECONDS"
            ),
        )

    def open_spider(self, spider) -> None:
        if not self.enabled:
            return

        missing = [ # looking for missing settings to raise error
            name
            for name, value in (
                ("MINIO_ENDPOINT", self.endpoint),
                ("MINIO_ACCESS_KEY", self.access_key),
                ("MINIO_SECRET_KEY", self.secret_key),
                ("MINIO_BUCKET", self.bucket),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                "Persistence is enabled but required MinIO settings are "
                f"missing: {', '.join(missing)}"
            )

        try:
            self.client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
            )
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
        except Exception:
            spider.crawler.stats.inc_value("errors/minio_connection")
            spider.logger.exception(
                "Unable to initialise MinIO bucket=%s endpoint=%s",
                self.bucket,
                self.endpoint,
            )
            raise

    def process_item(self, item, spider):
        document = ItemAdapter(item).asdict()
        # item adapter is built into scrapy, it converts the Scrapy yielded item into 
        # a dictionary regardless of its item type.
        raw_content = document.pop("raw_content", None)
        try:
            record_key = _build_record_key(document)
        except (TypeError, ValueError, UnicodeError) as exc:
            spider.crawler.stats.inc_value("errors/persistence_identity")
            spider.logger.error(
                "Cannot build persistence identity: url=%s detail=%s",
                document.get("landing_url") or document.get("doc_url"),
                exc,
            )
            raise DropItem("Document has no stable persistence identity") from exc

        document["record_key"] = record_key

        if not self.enabled:
            return document

        if not isinstance(raw_content, bytes) or not raw_content:
            spider.crawler.stats.inc_value("errors/minio_payload")
            spider.logger.error(
                "Original document bytes are missing: record_key=%s url=%s",
                record_key,
                document.get("doc_url"),
            )
            raise DropItem("Original document bytes are missing")

        source_format = document.get("source_format")
        if source_format not in {"html", "pdf"}:
            spider.crawler.stats.inc_value("errors/minio_payload")
            spider.logger.error(
                "Unsupported source format: record_key=%s format=%r",
                record_key,
                source_format,
            )
            raise DropItem("Source format must be either html or pdf")

        content_type = {
            "pdf": "application/pdf",
            "html": "text/html; charset=utf-8",
        }[source_format]
        object_name = "<unresolved>"
        try:
            blob_hash = hash_document(raw_content)
            object_name = _build_blob_object_name(
                record_key,
                self.prefix,
                source_format,
            )

            if self.client is None:
                raise RuntimeError("MinIO client was not initialised")

            metadata = {"blob-hash": blob_hash}
            content_hash = document.get("content_hash")
            if content_hash:
                metadata["content-hash"] = content_hash

            existing = _run_storage_operation(
                lambda: _stat_object_or_none(
                    self.client,
                    self.bucket,
                    object_name,
                ),
                spider=spider,
                operation_name="minio_stat",
                retry_times=self.retry_times,
                base_delay=self.retry_base_delay,
                max_delay=self.retry_max_delay,
            )
            if _metadata_blob_hash(existing) == blob_hash:
                etag = existing.etag
                persistence_outcome = "unchanged"
            else:
                result = _run_storage_operation(
                    lambda: self.client.put_object(
                        bucket_name=self.bucket,
                        object_name=object_name,
                        data=BytesIO(raw_content),
                        length=len(raw_content),
                        content_type=content_type,
                        metadata=metadata,
                    ),
                    spider=spider,
                    operation_name="minio_put",
                    retry_times=self.retry_times,
                    base_delay=self.retry_base_delay,
                    max_delay=self.retry_max_delay,
                )
                etag = result.etag
                persistence_outcome = (
                    "overwritten" if existing is not None else "inserted"
                )

            legacy_object_name = _build_legacy_blob_object_name(
                record_key,
                self.prefix,
            )
            legacy_removed = _run_storage_operation(
                lambda: _remove_object_if_present(
                    self.client,
                    self.bucket,
                    legacy_object_name,
                ),
                spider=spider,
                operation_name="minio_legacy_cleanup",
                retry_times=self.retry_times,
                base_delay=self.retry_base_delay,
                max_delay=self.retry_max_delay,
            )
            if legacy_removed:
                spider.crawler.stats.inc_value(
                    "persistence/minio_legacy_removed"
                )
            spider.crawler.stats.inc_value(
                f"persistence/minio_{persistence_outcome}"
            )

        except Exception as exc:
            spider.crawler.stats.inc_value("errors/minio_write")
            spider.logger.exception(
                "MinIO persistence failed: record_key=%s object=%s",
                record_key,
                object_name,
            )
            raise DropItem("MinIO persistence failed") from exc

        document["blob"] = {
            "bucket": self.bucket,
            "object_key": object_name,
            "etag": etag,
            "content_type": content_type,
            "size_bytes": len(raw_content),
            "sha256": blob_hash,
        }
        return document


class MongoPipeline:
    """Upsert raw ingestion metadata or fully scraped documents.

    A transient retry repeats the same stable ``record_key`` upsert, so an
    ambiguous network failure cannot create a duplicate logical document.
    """

    def __init__(
        self,
        enabled: bool,
        uri: str,
        database: str,
        collection: str,
        timeout_ms: int,
        retry_times: int = 2,
        retry_base_delay: float = 0.5,
        retry_max_delay: float = 5.0,
    ):
        self.enabled = enabled
        self.uri = uri
        self.database_name = database
        self.collection_name = collection
        self.timeout_ms = timeout_ms
        self.retry_times = retry_times
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.client: MongoClient | None = None
        self.collection = None

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            enabled=settings.getbool("PERSISTENCE_ENABLED"),
            uri=settings.get("MONGO_URI"),
            database=settings.get("MONGO_DATABASE"),
            collection=settings.get("MONGO_COLLECTION"),
            timeout_ms=settings.getint(
                "MONGO_SERVER_SELECTION_TIMEOUT_MS"
            ),
            retry_times=settings.getint("PERSISTENCE_RETRY_TIMES"),
            retry_base_delay=settings.getfloat(
                "PERSISTENCE_RETRY_BASE_DELAY_SECONDS"
            ),
            retry_max_delay=settings.getfloat(
                "PERSISTENCE_RETRY_MAX_DELAY_SECONDS"
            ),
        )

    def open_spider(self, spider) -> None:
        if not self.enabled:
            return

        missing = [
            name
            for name, value in (
                ("MONGO_URI", self.uri),
                ("MONGO_DATABASE", self.database_name),
                ("MONGO_COLLECTION", self.collection_name),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                "Persistence is enabled but required MongoDB settings are "
                f"missing: {', '.join(missing)}"
            )

        try:
            self.client = MongoClient(
                self.uri,
                serverSelectionTimeoutMS=self.timeout_ms,
            )
            self.client.admin.command("ping")
            database = self.client[self.database_name]
            self.collection = database[self.collection_name]

            # we'll make indices for faster querying
            self.collection.create_index(
                [("record_key", ASCENDING)],
                unique=True,
                name="uq_record_key",
            )
            self.collection.create_index(
                [("published_date", ASCENDING), ("category", ASCENDING)],
                name="published_date_category",
            )
            self.collection.create_index(
                [("partition_date", ASCENDING)],
                name="partition_date",
            )
            self.collection.create_index(
                [
                    ("partition_date", ASCENDING),
                    ("scraping_status", ASCENDING),
                ],
                name="partition_scraping_status",
            )
        except Exception:
            spider.crawler.stats.inc_value("errors/mongodb_connection")
            spider.logger.exception(
                "Unable to initialise MongoDB database=%s collection=%s",
                self.database_name,
                self.collection_name,
            )
            if self.client is not None:
                self.client.close()
            raise

    def process_item(self, item, spider):
        """Idempotently persist document metadata and scraping state in MongoDB."""

        document = ItemAdapter(item).asdict()

        if not self.enabled:
            return document
 
        record_key = document.get("record_key") # made in the prior pipeline above
        blob = document.get("blob") # produced as well in the prior pipeline
        if not record_key or not isinstance(blob, dict):
            spider.crawler.stats.inc_value("errors/mongodb_payload")
            spider.logger.error(
                "MongoDB item is missing persistence fields: url=%s",
                document.get("landing_url") or document.get("doc_url"),
            )
            raise DropItem("MongoDB item is missing persistence fields")

        if self.collection is None:
            spider.crawler.stats.inc_value("errors/mongodb_write")
            spider.logger.error("MongoDB collection was not initialised")
            raise DropItem("MongoDB collection was not initialised")

        now = datetime.now(timezone.utc)

        try:
            existing = _run_storage_operation(
                lambda: self.collection.find_one(
                    {"record_key": record_key},
                    {
                        "content_hash": 1,
                        "blob": 1,
                        "scraping_status": 1,
                    },
                ),
                spider=spider,
                operation_name="mongodb_find",
                retry_times=self.retry_times,
                base_delay=self.retry_base_delay,
                max_delay=self.retry_max_delay,
            )
            existing_blob = (existing or {}).get("blob") or {}

            blob_changed = (
                existing is None
                or existing_blob.get("sha256") != blob.get("sha256")
            ) #checks for   old blob SHA-256 != new blob SHA-256

            has_extracted_content  = bool(document.get("content_hash"))

            document["last_seen_at"] = now
            document["ingested_at"] = now
            update: dict = {
                "$set": document,
                "$setOnInsert": {"created_at": now},
                # Remove history created by older pipeline versions: this
                # collection now represents only the latest source state.
                "$unset": {"blob_history": ""},
            }

            if has_extracted_content :
                content_changed = (
                    existing is None
                    or existing.get("content_hash") != document["content_hash"] # diff content unlike blob changed (html metadata for ex not the text itself changed)
                    or blob_changed
                )

                document["scraping_status"] = "completed"
                document["scraped_blob_sha256"] = blob.get("sha256")
                document["scraped_at"] = now
                if content_changed:
                    document["content_updated_at"] = now
                update["$unset"]["scraping_error"] = ""
            else:
                content_changed = blob_changed
                current_status = (existing or {}).get("scraping_status")

                if blob_changed or current_status not in {
                    "completed",
                    "failed",
                }:
                    document["scraping_status"] = "pending"
                if blob_changed:
                    update["$unset"].update({
                        "content": "",
                        "content_hash": "",
                        "content_updated_at": "",
                        "scraped_at": "",
                        "scraped_blob_sha256": "",
                        "scraping_error": "",
                    })

            result = _run_storage_operation(
                lambda: self.collection.update_one(
                    {"record_key": record_key},
                    update,
                    upsert=True,
                ),
                spider=spider,
                operation_name="mongodb_upsert",
                retry_times=self.retry_times,
                base_delay=self.retry_base_delay,
                max_delay=self.retry_max_delay,
            )

            if result.upserted_id is not None:
                spider.crawler.stats.inc_value("persistence/mongodb_inserted")
            elif content_changed:
                spider.crawler.stats.inc_value("persistence/mongodb_updated")
            else:
                spider.crawler.stats.inc_value("persistence/mongodb_unchanged")

        except Exception as exc:
            spider.crawler.stats.inc_value("errors/mongodb_write")
            spider.logger.exception(
                "MongoDB upsert failed: record_key=%s url=%s",
                record_key,
                document.get("landing_url"),
            )
            raise DropItem("MongoDB upsert failed") from exc

        return document

    def close_spider(self, spider) -> None:
        if self.client is not None:
            self.client.close()
