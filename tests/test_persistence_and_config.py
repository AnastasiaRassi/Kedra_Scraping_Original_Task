from __future__ import annotations

import json
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

from kedra_scraper.config import apply_source_settings, load_source_registry

try:
    from kedra_scraper.pipelines import (
        MinioPipeline,
        MongoPipeline,
        _build_blob_object_name,
        _retry_with_exponential_backoff,
    )
    from kedra_scraper.file_content import _download_blob
except ModuleNotFoundError as exc:
    persistence_dependencies = {
        "itemadapter",
        "minio",
        "pymongo",
        "pypdf",
        "scrapy",
        "twisted",
    }
    if exc.name not in persistence_dependencies:
        raise
    PIPELINE_IMPORT_ERROR = str(exc)
else:
    PIPELINE_IMPORT_ERROR = None


class _Stats:
    def __init__(self) -> None:
        self.values: dict[str, int] = {}

    def inc_value(self, key: str) -> None:
        self.values[key] = self.values.get(key, 0) + 1


class _Logger:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def error(self, *args, **kwargs) -> None:
        pass

    def exception(self, *args, **kwargs) -> None:
        pass

    def log(self, level, message, **kwargs) -> None:
        extra = kwargs.get("extra", {})
        self.events.append(
            {
                "level": level,
                "message": message,
                "event": extra.get("event"),
                **extra.get("structured_fields", {}),
            }
        )


class _Spider:
    def __init__(self) -> None:
        self.crawler = SimpleNamespace(stats=_Stats())
        self.logger = _Logger()


class _Settings(dict):
    def __init__(self, **values) -> None:
        super().__init__(values)
        self.applied: dict[str, tuple[str, str]] = {}

    def set(self, name: str, value: str, priority: str) -> None:
        self.applied[name] = (value, priority)


class _ExistingMinioClient:
    def __init__(self, blob_hash: str) -> None:
        self.blob_hash = blob_hash
        self.object_names: list[str] = []
        self.removed: list[str] = []

    def stat_object(self, bucket: str, object_name: str):
        self.object_names.append(object_name)
        return SimpleNamespace(
            metadata={"x-amz-meta-blob-hash": self.blob_hash},
            etag="existing-etag",
        )

    def put_object(self, **kwargs):
        raise AssertionError("Unchanged objects must not be uploaded")

    def remove_object(self, bucket: str, object_name: str) -> None:
        self.removed.append(object_name)


class _ChangedCurrentMinioClient:
    def __init__(self) -> None:
        self.uploaded: list[str] = []
        self.removed: list[str] = []

    def stat_object(self, bucket: str, object_name: str):
        return SimpleNamespace(
            metadata={"x-amz-meta-blob-hash": "previous-hash"},
            etag="existing-etag",
        )

    def put_object(self, **kwargs):
        self.uploaded.append(kwargs["object_name"])
        return SimpleNamespace(etag="updated-etag")

    def remove_object(self, bucket: str, object_name: str) -> None:
        self.removed.append(object_name)


class _RetryingMinioClient(_ChangedCurrentMinioClient):
    def __init__(self) -> None:
        super().__init__()
        self.put_attempts = 0

    def put_object(self, **kwargs):
        self.put_attempts += 1
        if self.put_attempts == 1:
            raise OSError("temporary MinIO connection loss")
        return super().put_object(**kwargs)


class _BlobResponse:
    def read(self) -> bytes:
        return b"current bytes"

    def close(self) -> None:
        pass

    def release_conn(self) -> None:
        pass


class _BlobClient:
    def __init__(self) -> None:
        self.object_key: str | None = None

    def get_object(self, bucket: str, object_key: str):
        self.object_key = object_key
        return _BlobResponse()


class _MongoCollection:
    def __init__(self, existing: dict | None = None) -> None:
        self.existing = existing
        self.update: dict | None = None

    def find_one(self, *args, **kwargs):
        return self.existing

    def update_one(self, query: dict, update: dict, upsert: bool):
        self.update = update
        return SimpleNamespace(upserted_id="inserted")


class _RetryingMongoCollection(_MongoCollection):
    def __init__(self, existing: dict | None = None) -> None:
        super().__init__(existing)
        self.update_attempts = 0

    def update_one(self, query: dict, update: dict, upsert: bool):
        self.update_attempts += 1
        if self.update_attempts == 1:
            raise OSError("temporary MongoDB connection loss")
        return super().update_one(query, update, upsert)


@unittest.skipIf(
    PIPELINE_IMPORT_ERROR is not None,
    f"persistence dependencies are unavailable: {PIPELINE_IMPORT_ERROR}",
)
class PersistenceUpsertTests(unittest.TestCase):
    def test_object_name_is_a_flat_file_with_its_format(self) -> None:
        self.assertEqual(
            _build_blob_object_name("record", "documents", "pdf"),
            "documents/record.pdf",
        )

    def test_unchanged_blob_is_reused(self) -> None:
        raw_content = b"unchanged source bytes"
        blob_hash = sha256(raw_content).hexdigest()
        client = _ExistingMinioClient(blob_hash)
        pipeline = MinioPipeline(
            enabled=True,
            endpoint="unused",
            access_key="unused",
            secret_key="unused",
            secure=False,
            bucket="raw-documents",
            prefix="documents",
        )
        pipeline.client = client

        document = pipeline.process_item(
            {
                "source": "https://example.test",
                "landing_url": "https://example.test/document/1",
                "doc_url": "https://example.test/document/1.pdf",
                "source_format": "pdf",
                "raw_content": raw_content,
            },
            _Spider(),
        )

        object_name = f"documents/{document['record_key']}.pdf"
        legacy_name = f"documents/{document['record_key']}/current"
        self.assertEqual(client.object_names, [object_name, legacy_name])
        self.assertEqual(client.removed, [legacy_name])
        self.assertEqual(document["blob"]["object_key"], object_name)
        self.assertNotIn("version_object_key", document["blob"])
        self.assertEqual(document["blob"]["sha256"], blob_hash)

    def test_changed_blob_overwrites_only_the_current_object(self) -> None:
        raw_content = b"new source bytes"
        blob_hash = sha256(raw_content).hexdigest()
        client = _ChangedCurrentMinioClient()
        pipeline = MinioPipeline(
            enabled=True,
            endpoint="unused",
            access_key="unused",
            secret_key="unused",
            secure=False,
            bucket="raw-documents",
            prefix="documents",
        )
        pipeline.client = client

        document = pipeline.process_item(
            {
                "source": "https://example.test",
                "landing_url": "https://example.test/document/1",
                "doc_url": "https://example.test/document/1.pdf",
                "source_format": "pdf",
                "raw_content": raw_content,
            },
            _Spider(),
        )

        self.assertEqual(
            client.uploaded,
            [f"documents/{document['record_key']}.pdf"],
        )
        self.assertEqual(
            client.removed,
            [f"documents/{document['record_key']}/current"],
        )

    def test_transient_minio_write_is_retried_and_reported(self) -> None:
        client = _RetryingMinioClient()
        pipeline = MinioPipeline(
            enabled=True,
            endpoint="unused",
            access_key="unused",
            secret_key="unused",
            secure=False,
            bucket="raw-documents",
            prefix="documents",
            retry_times=2,
            retry_base_delay=0,
            retry_max_delay=0,
        )
        pipeline.client = client
        spider = _Spider()

        pipeline.process_item(
            {
                "source": "https://example.test",
                "landing_url": "https://example.test/document/1",
                "doc_url": "https://example.test/document/1.pdf",
                "source_format": "pdf",
                "raw_content": b"new source bytes",
            },
            spider,
        )

        self.assertEqual(client.put_attempts, 2)
        self.assertEqual(
            spider.crawler.stats.values["persistence/retry_attempts"],
            1,
        )
        self.assertEqual(
            spider.crawler.stats.values["persistence/retry_recovered"],
            1,
        )
        self.assertEqual(spider.logger.events[0]["operation"], "minio_put")

    def test_mongodb_removes_legacy_blob_history(self) -> None:
        blob = {
            "bucket": "raw-documents",
            "object_key": "documents/record.html",
            "etag": "storage-specific-etag",
            "content_type": "text/html; charset=utf-8",
            "size_bytes": 42,
            "sha256": "hash",
        }
        collection = _MongoCollection(
            existing={"blob": blob, "blob_history": [{"sha256": "old"}]}
        )
        pipeline = MongoPipeline(
            enabled=True,
            uri="unused",
            database="unused",
            collection="unused",
            timeout_ms=1,
        )
        pipeline.collection = collection

        pipeline.process_item(
            {"record_key": "record", "blob": blob},
            _Spider(),
        )

        self.assertIsNotNone(collection.update)
        self.assertNotIn("$addToSet", collection.update)
        self.assertEqual(collection.update["$unset"]["blob_history"], "")
        self.assertEqual(collection.update["$set"]["blob"], blob)

    def test_transient_mongodb_upsert_is_retried_and_reported(self) -> None:
        blob = {
            "bucket": "raw-documents",
            "object_key": "documents/record.html",
            "sha256": "hash",
        }
        collection = _RetryingMongoCollection(existing=None)
        pipeline = MongoPipeline(
            enabled=True,
            uri="unused",
            database="unused",
            collection="unused",
            timeout_ms=1,
            retry_times=2,
            retry_base_delay=0,
            retry_max_delay=0,
        )
        pipeline.collection = collection
        spider = _Spider()

        pipeline.process_item(
            {"record_key": "record", "blob": blob},
            spider,
        )

        self.assertEqual(collection.update_attempts, 2)
        self.assertEqual(
            spider.crawler.stats.values[
                "persistence/retry_attempts/mongodb_upsert"
            ],
            1,
        )
        self.assertEqual(
            spider.crawler.stats.values[
                "persistence/retry_recovered/mongodb_upsert"
            ],
            1,
        )

    def test_backoff_doubles_until_the_configured_cap(self) -> None:
        attempts = 0
        delays: list[float] = []

        def operation() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 5:
                raise OSError("temporary failure")
            return "recovered"

        result = _retry_with_exponential_backoff(
            operation,
            retry_times=4,
            base_delay=1,
            max_delay=3,
            should_retry=lambda exc: isinstance(exc, OSError),
            sleeper=delays.append,
        )

        self.assertEqual(result, "recovered")
        self.assertEqual(delays, [1, 2, 3, 3])

    def test_non_transient_failure_is_not_retried(self) -> None:
        attempts = 0
        delays: list[float] = []

        def invalid_operation() -> None:
            nonlocal attempts
            attempts += 1
            raise ValueError("invalid payload")

        with self.assertRaisesRegex(ValueError, "invalid payload"):
            _retry_with_exponential_backoff(
                invalid_operation,
                retry_times=3,
                base_delay=1,
                max_delay=5,
                should_retry=lambda exc: isinstance(exc, OSError),
                sleeper=delays.append,
            )

        self.assertEqual(attempts, 1)
        self.assertEqual(delays, [])

    def test_scraper_reads_the_current_object(self) -> None:
        client = _BlobClient()
        content = _download_blob(
            client,
            {
                "bucket": "raw-documents",
                "object_key": "documents/record.pdf",
            },
        )

        self.assertEqual(content, b"current bytes")
        self.assertEqual(client.object_key, "documents/record.pdf")


class SiteConfigTests(unittest.TestCase):
    def test_registry_loads_html_selectors_from_site_config(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            site_config_path = root / "site.json"
            site_config_path.write_text(
                json.dumps(
                    {
                        "selectors": {
                            "html_content": ["main article", "div.document"]
                        }
                    }
                ),
                encoding="utf-8",
            )
            registry_path = root / "sources.json"
            registry_path.write_text(
                json.dumps(
                    {
                        "sources": {
                            "example": {
                                "spider": "example_spider",
                                "source": "https://example.test",
                                "spider_settings": {
                                    "EXAMPLE_CONFIG_PATH": str(site_config_path),
                                    "CONCURRENT_REQUESTS_PER_DOMAIN": "5",
                                },
                                "site_config_setting": "EXAMPLE_CONFIG_PATH",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            source = load_source_registry(str(registry_path))["example"]
            settings = _Settings(SOURCE_REGISTRY_PATH=str(registry_path))
            apply_source_settings(settings, "example")

        self.assertEqual(source.site_config_path, str(site_config_path))
        self.assertEqual(
            source.html_content_selectors,
            ("main article", "div.document"),
        )
        self.assertEqual(
            settings.applied["CONCURRENT_REQUESTS_PER_DOMAIN"],
            ("5", "spider"),
        )


if __name__ == "__main__":
    unittest.main()
