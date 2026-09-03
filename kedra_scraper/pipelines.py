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


class MinioPipeline:
    """Store original document bytes and add their MinIO reference."""

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

        missing = [
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

        # Keep JSON test exports serialisable without requiring either service.
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
        extension = "pdf" if source_format == "pdf" else "html"
        content_type = (
            "application/pdf"
            if source_format == "pdf"
            else "text/html; charset=utf-8"
        )
        object_name = f"{record_key}.{extension}"
        if self.prefix:
            object_name = f"{self.prefix}/{object_name}"

        try:
            blob_hash = hash_document(raw_content)

            if self.client is None:
                raise RuntimeError("MinIO client was not initialised")

            existing = None
            try:
                existing = self.client.stat_object(self.bucket, object_name)
            except S3Error as exc:
                if exc.code not in {"NoSuchKey", "NoSuchObject"}:
                    raise

            existing_hash = None
            if existing is not None:
                existing_hash = (
                    existing.metadata.get("x-amz-meta-blob-hash")
                    or existing.metadata.get("X-Amz-Meta-Blob-Hash")
                    or existing.metadata.get("blob-hash")
                )

            if existing is not None and existing_hash == blob_hash:
                etag = existing.etag
                spider.crawler.stats.inc_value(
                    "persistence/minio_unchanged"
                )
            else:
                result = self.client.put_object(
                    bucket_name=self.bucket,
                    object_name=object_name,
                    data=BytesIO(raw_content),
                    length=len(raw_content),
                    content_type=content_type,
                    metadata={
                        "blob-hash": blob_hash,
                        "content-hash": document["content_hash"],
                    },
                )
                etag = result.etag
                spider.crawler.stats.inc_value("persistence/minio_uploaded")

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
    """Upsert extracted documents and their MinIO references into MongoDB."""

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
        document = ItemAdapter(item).asdict()

        if not self.enabled:
            return document

        record_key = document.get("record_key")
        blob = document.get("blob")
        if not record_key or not isinstance(blob, dict):
            spider.crawler.stats.inc_value("errors/mongodb_payload")
            spider.logger.error(
                "MongoDB item is missing persistence fields: url=%s",
                document.get("landing_url") or document.get("doc_url"),
            )
            raise DropItem("MongoDB item is missing persistence fields")

        if self.collection is None:
            spider.crawler.stats.inc_value("errors/mongodb_write")
            spider.logger.error(
                "MongoDB collection was not initialised"
            )
            raise DropItem("MongoDB collection was not initialised")

        now = datetime.now(timezone.utc)

        try:
            existing = self.collection.find_one(
                {"record_key": record_key},
                {"content_hash": 1, "blob.sha256": 1},
            )
            content_changed = (
                existing is None
                or existing.get("content_hash") != document["content_hash"]
                or existing.get("blob", {}).get("sha256")
                != blob.get("sha256")
            )

            document["last_seen_at"] = now
            if content_changed:
                document["content_updated_at"] = now

            result = self.collection.update_one(
                {"record_key": record_key},
                {
                    "$set": document,
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )

            if result.upserted_id is not None:
                spider.crawler.stats.inc_value(
                    "persistence/mongodb_inserted"
                )
            elif content_changed:
                spider.crawler.stats.inc_value(
                    "persistence/mongodb_updated"
                )
            else:
                spider.crawler.stats.inc_value(
                    "persistence/mongodb_unchanged"
                )

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
