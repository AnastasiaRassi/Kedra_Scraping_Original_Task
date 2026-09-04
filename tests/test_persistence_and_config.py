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
        _build_blob_history_entry,
        _build_blob_object_name,
        _build_current_blob_object_name,
    )
    from kedra_scraper.scraping import _download_blob
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
    def error(self, *args, **kwargs) -> None:
        pass

    def exception(self, *args, **kwargs) -> None:
        pass


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

    def stat_object(self, bucket: str, object_name: str):
        self.object_names.append(object_name)
        return SimpleNamespace(
            metadata={"x-amz-meta-blob-hash": self.blob_hash},
            etag="existing-etag",
        )

    def put_object(self, **kwargs):
        raise AssertionError("Unchanged objects must not be uploaded")


class _ChangedCurrentMinioClient:
    def __init__(self, blob_hash: str) -> None:
        self.blob_hash = blob_hash
        self.uploaded: list[str] = []

    def stat_object(self, bucket: str, object_name: str):
        stored_hash = (
            "previous-hash" if object_name.endswith("/current")
            else self.blob_hash
        )
        return SimpleNamespace(
            metadata={"x-amz-meta-blob-hash": stored_hash},
            etag="existing-etag",
        )

    def put_object(self, **kwargs):
        self.uploaded.append(kwargs["object_name"])
        return SimpleNamespace(etag="updated-etag")


class _BlobResponse:
    def read(self) -> bytes:
        return b"version bytes"

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


@unittest.skipIf(
    PIPELINE_IMPORT_ERROR is not None,
    f"persistence dependencies are unavailable: {PIPELINE_IMPORT_ERROR}",
)
class PersistenceHistoryTests(unittest.TestCase):
    def test_object_name_is_content_addressed(self) -> None:
        first = _build_blob_object_name("record", "aaa", "pdf", "documents")
        second = _build_blob_object_name("record", "bbb", "pdf", "documents")

        self.assertEqual(first, "documents/record/aaa.pdf")
        self.assertEqual(second, "documents/record/bbb.pdf")
        self.assertNotEqual(first, second)
        self.assertEqual(
            _build_current_blob_object_name("record", "documents"),
            "documents/record/current",
        )

    def test_existing_blob_version_is_reused(self) -> None:
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

        version_object = (
            f"documents/{document['record_key']}/{blob_hash}.pdf"
        )
        current_object = f"documents/{document['record_key']}/current"
        self.assertEqual(client.object_names, [version_object, current_object])
        self.assertEqual(document["blob"]["object_key"], current_object)
        self.assertEqual(
            document["blob"]["version_object_key"],
            version_object,
        )
        self.assertEqual(document["blob"]["sha256"], blob_hash)

    def test_changed_blob_overwrites_only_the_current_object(self) -> None:
        raw_content = b"new source bytes"
        blob_hash = sha256(raw_content).hexdigest()
        client = _ChangedCurrentMinioClient(blob_hash)
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
            [f"documents/{document['record_key']}/current"],
        )

    def test_mongodb_adds_stable_blob_history_entry(self) -> None:
        blob = {
            "bucket": "raw-documents",
            "object_key": "documents/record/current",
            "version_object_key": "documents/record/hash.html",
            "etag": "storage-specific-etag",
            "content_type": "text/html; charset=utf-8",
            "size_bytes": 42,
            "sha256": "hash",
        }
        collection = _MongoCollection()
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
        history = collection.update["$addToSet"]["blob_history"]["$each"]
        self.assertEqual(history, [_build_blob_history_entry(blob)])
        self.assertEqual(history[0]["object_key"], blob["version_object_key"])
        self.assertNotIn("etag", history[0])
        self.assertEqual(collection.update["$set"]["blob"], blob)

    def test_mongodb_seeds_history_with_the_previous_blob(self) -> None:
        previous_blob = {
            "bucket": "raw-documents",
            "object_key": "documents/record.html",
            "etag": "legacy-etag",
            "content_type": "text/html; charset=utf-8",
            "size_bytes": 10,
            "sha256": "old-hash",
        }
        current_blob = {
            "bucket": "raw-documents",
            "object_key": "documents/record/current",
            "version_object_key": "documents/record/new-hash.html",
            "etag": "new-etag",
            "content_type": "text/html; charset=utf-8",
            "size_bytes": 12,
            "sha256": "new-hash",
        }
        collection = _MongoCollection(existing={"blob": previous_blob})
        pipeline = MongoPipeline(
            enabled=True,
            uri="unused",
            database="unused",
            collection="unused",
            timeout_ms=1,
        )
        pipeline.collection = collection

        pipeline.process_item(
            {"record_key": "record", "blob": current_blob},
            _Spider(),
        )

        history = collection.update["$addToSet"]["blob_history"]["$each"]
        self.assertEqual(
            history,
            [
                _build_blob_history_entry(previous_blob),
                _build_blob_history_entry(current_blob),
            ],
        )

    def test_scraper_reads_the_immutable_version(self) -> None:
        client = _BlobClient()
        content = _download_blob(
            client,
            {
                "bucket": "raw-documents",
                "object_key": "documents/record/current",
                "version_object_key": "documents/record/hash.html",
            },
        )

        self.assertEqual(content, b"version bytes")
        self.assertEqual(client.object_key, "documents/record/hash.html")


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
