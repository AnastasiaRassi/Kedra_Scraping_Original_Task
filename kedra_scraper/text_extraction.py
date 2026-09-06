"""
This script takes raw files (PDFs and web pages) stored in cloud storage, pulls out all the 
readable plain text, and saves that text into a database so it can be searched or analyzed.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

from minio import Minio
from pypdf import PdfReader
from pymongo import MongoClient
from scrapy import Selector

from kedra_scraper.source_registry import SourceRegistryEntry
from kedra_scraper.utils import env_bool, env_int, hash_document


def scrape_partition(
    source_config: SourceRegistryEntry,
    partition_date: str,
) -> dict[str, int]:
    """This function handles the text extraction phase for a single source/month partition. It fetches
     raw binary files from MinIO, extracts their plain text, and updates their records in MongoDB."""
    
    mongo_uri = _required_env("MONGO_URI")
    database_name = _required_env("MONGO_DATABASE")
    collection_name = _required_env("MONGO_COLLECTION")
    minio_endpoint = _required_env("MINIO_ENDPOINT")
    minio_access_key = _required_env("MINIO_ACCESS_KEY")
    minio_secret_key = _required_env("MINIO_SECRET_KEY")
    timeout_ms = env_int(
        "MONGO_SERVER_SELECTION_TIMEOUT_MS",
        5000,
        minimum=1,
    )

    mongo_client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=timeout_ms,
    )
    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=env_bool("MINIO_SECURE", False),
    )

    summary = {
        "raw_documents": 0,
        "scraped": 0,
        "unchanged": 0,
        "scraping_failed": 0,
    }

    try:
        mongo_client.admin.command("ping")
        collection = mongo_client[database_name][collection_name]
        cursor = collection.find(
            {
                "source": source_config.source,
                "partition_date": partition_date,
                "blob.object_key": {"$exists": True},
            }
        )

        try:
            for document in cursor:
                summary["raw_documents"] += 1
                blob = document.get("blob") or {}
                blob_hash = blob.get("sha256")

                if (
                    document.get("scraping_status") == "completed"
                    and document.get("scraped_blob_sha256") == blob_hash
                    and document.get("content_hash")
                ):
                    summary["unchanged"] += 1
                    continue

                try:
                    raw_content = _download_blob(minio_client, blob)
                    if hash_document(raw_content) != blob_hash:
                        raise RuntimeError(
                            "Downloaded blob hash does not match MongoDB metadata"
                        )
                except (KeyError, TypeError, ValueError) as exc:
                    _mark_scraping_failed(collection, document, exc)
                    summary["scraping_failed"] += 1
                    continue

                try:
                    content = _extract_content(
                        raw_content,
                        document.get("source_format"),
                        source_config.html_content_selectors,
                    )
                    content_hash = hash_document(content)
                except Exception as exc:
                    _mark_scraping_failed(collection, document, exc)
                    summary["scraping_failed"] += 1
                    continue

                now = datetime.now(timezone.utc)
                collection.update_one(
                    {"record_key": document["record_key"]},
                    {
                        "$set": {
                            "content": content,
                            "content_hash": content_hash,
                            "scraping_status": "completed",
                            "scraped_blob_sha256": blob_hash,
                            "scraped_at": now,
                            "content_updated_at": now,
                        },
                        "$unset": {"scraping_error": ""},
                    },
                )
                summary["scraped"] += 1
        finally:
            cursor.close()
    finally:
        mongo_client.close()

    return summary


def _download_blob(client: Minio, blob: dict[str, Any]) -> bytes:
    bucket = blob.get("bucket")
    object_key = blob.get("object_key")
    if not isinstance(bucket, str) or not bucket:
        raise ValueError("MongoDB record has no MinIO bucket")
    if not isinstance(object_key, str) or not object_key:
        raise ValueError("MongoDB record has no MinIO object key")

    response = client.get_object(bucket, object_key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def _extract_content(
    raw_content: bytes,
    source_format: Any,
    html_content_selectors: tuple[str, ...],
) -> str:
    if source_format == "pdf":
        reader = PdfReader(BytesIO(raw_content))
        content = "\n\n".join(
            text
            for page in reader.pages
            if (text := (page.extract_text() or "").strip())
        )
    elif source_format == "html":
        selector = Selector(
            text=raw_content.decode("utf-8", errors="replace"),
            type="html",
        )
        content = ""
        for css_selector in html_content_selectors:
            nodes = selector.css(css_selector)
            if nodes:
                content = _clean_text(
                    " ".join(nodes.xpath(".//text()").getall())
                )
                if content:
                    break
    else:
        raise ValueError(f"Unsupported source_format: {source_format!r}")

    if not content:
        raise ValueError(
            "Document contains no extractable text; it may require OCR"
        )
    return content


def _mark_scraping_failed(collection, document: dict, exc: Exception) -> None:
    error_limit = env_int(
        "SCRAPING_ERROR_MAX_CHARS",
        500,
        minimum=50,
    )
    message = f"{type(exc).__name__}: {exc}"[:error_limit]
    collection.update_one(
        {"record_key": document["record_key"]},
        {
            "$set": {
                "scraping_status": "failed",
                "scraping_error": message,
                "scraping_attempted_at": datetime.now(timezone.utc),
            }
        },
    )


def _clean_text(value: str | None) -> str:
    return " ".join((value or "").split())


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} is required for document scraping")
    return value

