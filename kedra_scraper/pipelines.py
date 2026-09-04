from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO

from itemadapter import ItemAdapter
from minio import Minio
from minio.error import S3Error
from pymongo import ASCENDING, MongoClient
from scrapy.exceptions import DropItem

from kedra_scraper.utils import hash_document


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
) -> str:
    """Build the stable MinIO key for one logical source document."""
    object_name = f"{record_key}/current"
    return f"{prefix}/{object_name}" if prefix else object_name


def _stat_object_or_none(client: Minio, bucket: str, object_name: str):
    try:
        return client.stat_object(bucket, object_name)
    except S3Error as exc:
        if exc.code not in {"NoSuchKey", "NoSuchObject"}:
            raise
        return None


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
    ):
        self.enabled = enabled
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.bucket = bucket
        self.prefix = prefix.strip("/")
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
        content_type = (
            "application/pdf"
            if source_format == "pdf"
            else "text/html; charset=utf-8"
        )
        object_name = "<unresolved>"
        try:
            blob_hash = hash_document(raw_content)
            object_name = _build_blob_object_name(
                record_key,
                self.prefix,
            )

            if self.client is None:
                raise RuntimeError("MinIO client was not initialised")

            metadata = {"blob-hash": blob_hash}
            content_hash = document.get("content_hash")
            if content_hash:
                metadata["content-hash"] = content_hash

            existing = _stat_object_or_none(
                self.client,
                self.bucket,
                object_name,
            )
            if _metadata_blob_hash(existing) == blob_hash:
                etag = existing.etag
                spider.crawler.stats.inc_value("persistence/minio_unchanged")
            else:
                result = self.client.put_object(
                    bucket_name=self.bucket,
                    object_name=object_name,
                    data=BytesIO(raw_content),
                    length=len(raw_content),
                    content_type=content_type,
                    metadata=metadata,
                )
                etag = result.etag
                spider.crawler.stats.inc_value(
                    "persistence/minio_overwritten"
                    if existing is not None
                    else "persistence/minio_inserted"
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
    """Upsert raw ingestion metadata or fully scraped documents."""

    def __init__(
        self,
        enabled: bool,
        uri: str,
        database: str,
        collection: str,
        timeout_ms: int,
    ):
        self.enabled = enabled
        self.uri = uri
        self.database_name = database
        self.collection_name = collection
        self.timeout_ms = timeout_ms
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
            existing = self.collection.find_one(
                {"record_key": record_key},
                {
                    "content_hash": 1,
                    "blob": 1,
                    "scraping_status": 1,
                },
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

            result = self.collection.update_one(
                {"record_key": record_key},
                update,
                upsert=True,
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
