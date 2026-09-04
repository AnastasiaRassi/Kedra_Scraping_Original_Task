# Kedra Scraping Pipeline

A Scrapy and Dagster pipeline for collecting legal documents from multiple
sources, preserving each original file, and scraping its text into a shared
MongoDB document schema. The Irish Workplace Relations Commission (WRC) is the
first implemented source.

## Current status

The project currently supports:

- Required `start_date` and `end_date` crawl inputs.
- Configurable calendar-month partitioning.
- WRC searches across all four Body categories.
- Search-result pagination.
- HTML documents, direct PDFs, and HTML wrappers containing PDF downloads.
- A shared raw-item and scraped-item contract.
- Source-specific metadata through `source_metadata`.
- SHA-256 hashes for both original bytes and scraped text.
- Idempotent MinIO and MongoDB persistence.
- Source/month Dagster partitions.
- Separate `raw_documents` and `scraped_documents` Dagster assets.
- Scrapy request retries plus Dagster task retries.
- Machine-readable crawl reconciliation summaries.

Persistence is disabled by default so small JSON test crawls do not require
storage services.

## Architecture

Dagster orchestrates two dependent assets for each `source` and calendar
`date` partition:

1. `raw_documents` runs the configured Scrapy spider in ingestion mode. It
   stores the original HTML/PDF bytes in MinIO and upserts source metadata in
   MongoDB with `scraping_status="pending"`.
2. `scraped_documents` runs only after `raw_documents` succeeds. It reads the
   original MinIO objects, extracts text according to their format and source
   rules, and updates the MongoDB records with `content`, `content_hash`, and
   `scraping_status="completed"`.

This is ingestion followed by scraping. The second asset is deliberately
named `scraped_documents`; its responsibility is document text extraction.

The Dagster definitions are source-agnostic. `config/sources.json` maps a
source key to its spider, spider settings, canonical source URL, and the
setting that points to that site's configuration file. HTML selectors and
other website-specific rules live only in the site's own configuration file.

## Installation

Python 3.11 or later is recommended.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Run commands from the repository root, where `scrapy.cfg` is located.

Copy the environment template:

```powershell
Copy-Item .env.example .env
```

The real `.env` is ignored by Git. Do not commit it.

## Local storage services

MongoDB and MinIO run as Docker containers. The credentials in `.env` are
used both by Docker Compose when the containers are first created and by the
Python application when it connects.

```powershell
docker compose config
docker compose up -d
docker compose ps
```

The default endpoints are:

- MongoDB: `localhost:27017`
- MinIO API: `localhost:9000`
- MinIO console: `http://localhost:9001`

`MONGO_DATA_PATH` and `MINIO_DATA_PATH` can be Docker volume names or host
paths. The defaults use persistent named volumes. If MongoDB was already
initialised, changing its username or password in `.env` does not rewrite the
credentials stored in the existing volume.

## Configuration

All connection strings, storage locations, partition settings, crawl tuning,
and Dagster retry settings are configurable through `.env` or JSON source
configuration. No local credentials belong in source code.

| Area | Main variables |
| --- | --- |
| Storage switch | `PERSISTENCE_ENABLED` |
| Storage retries | `PERSISTENCE_RETRY_TIMES`, `PERSISTENCE_RETRY_BASE_DELAY_SECONDS`, `PERSISTENCE_RETRY_MAX_DELAY_SECONDS` |
| MongoDB | `MONGO_URI`, `MONGO_DATABASE`, `MONGO_COLLECTION`, `MONGO_SERVER_SELECTION_TIMEOUT_MS` |
| MinIO | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_SECURE`, `MINIO_BUCKET`, `MINIO_PREFIX` |
| Docker | Image, container, bind-host, port, credential, and data-path variables in `.env.example` |
| Direct crawl period | `SCRAPE_START_DATE`, `SCRAPE_END_DATE`, `SCRAPE_PARTITION_MONTHS` |
| Source rules | `WRC_CONFIG_PATH`, `SOURCE_REGISTRY_PATH` |
| Request behavior | `SCRAPY_ROBOTSTXT_OBEY`, `SCRAPY_COOKIES_ENABLED`, `SCRAPY_DOWNLOAD_TIMEOUT`, `SCRAPY_RETRY_TIMES` |
| Structured logs | `SCRAPY_LOG_DIR`, `SCRAPY_STRUCTURED_LOG_PATH`, `SCRAPY_LOG_MAX_BYTES`, `SCRAPY_LOG_BACKUP_COUNT` |
| Concurrency | `SCRAPY_CONCURRENT_REQUESTS`, `SCRAPY_CONCURRENT_REQUESTS_PER_DOMAIN`, `SCRAPY_DOWNLOAD_DELAY` |
| AutoThrottle | `SCRAPY_AUTOTHROTTLE_ENABLED`, `SCRAPY_AUTOTHROTTLE_START_DELAY`, `SCRAPY_AUTOTHROTTLE_MAX_DELAY`, `SCRAPY_AUTOTHROTTLE_TARGET_CONCURRENCY` |
| Dagster partitions | `DAGSTER_PARTITION_START_DATE`, `DAGSTER_PARTITION_END_DATE`, `DAGSTER_PARTITION_TIMEZONE`, `DAGSTER_PARTITION_END_OFFSET` |
| Dagster retries | `DAGSTER_CRAWL_MAX_RETRIES`, `DAGSTER_CRAWL_RETRY_DELAY_SECONDS`, `DAGSTER_CRAWL_TIMEOUT_SECONDS` |
| Dagster validation | `DAGSTER_ALLOWED_CLOSE_REASONS`, `DAGSTER_MAX_REQUEST_FAILURES`, `DAGSTER_MAX_PERSISTENCE_ERRORS`, `DAGSTER_MAX_UNEXPLAINED_MISSING` |
| Schedule | `DAGSTER_SCHEDULE_CRON` |

All supported variables and development defaults are listed in
`.env.example`. Numeric and Boolean values are validated early.

### Source registry

`config/sources.json` is the orchestration registry. Its keys become Dagster's
`source` partitions:

```json
{
  "sources": {
    "wrc_ie": {
      "spider": "WRC_IE",
      "source": "https://www.workplacerelations.ie",
      "spider_settings": {
        "WRC_CONFIG_PATH": "config/wrc.json",
        "CONCURRENT_REQUESTS_PER_DOMAIN": "6",
        "DOWNLOAD_DELAY": "0.1",
        "AUTOTHROTTLE_ENABLED": "true",
        "AUTOTHROTTLE_TARGET_CONCURRENCY": "3.0"
      },
      "site_config_setting": "WRC_CONFIG_PATH"
    }
  }
}
```

The `spider_settings` values override the conservative global request-rate
defaults for that source. Direct Scrapy crawls, profiler runs using `--source`,
and Dagster ingestion all load the same overrides. Explicit command-line
`-s NAME=VALUE` settings have the highest priority and are intended for
experiments.

Global rate settings must remain conservative because a benchmark against one
website says nothing about another website's capacity or blocking policy. Add
rate overrides for a new source only after benchmarking that source on a
representative complete partition.

To add another website, implement a source spider that accepts
`start_date`/`end_date`, yields the shared raw item in ingestion mode, and
writes the shared crawl-summary counters. Then add its entry to the registry.
The Dagster assets do not need website-specific branches.

## Run a direct Scrapy test

Direct crawls default to `SCRAPE_MODE=full`: the spider downloads each file,
extracts its text, and yields a complete `KedraScraperItem` in one run.

The WRC spider accepts `DD-MM-YYYY` or `YYYY-MM-DD` dates. It
automatically loads the `wrc_ie` request-rate overrides from the source
registry; command-line `-s` values can still override them:

```powershell
scrapy crawl WRC_IE `
  -a start_date=01-01-2008 `
  -a end_date=31-03-2008 `
  -O wrc_test.jsonl `
  -s LOG_LEVEL=INFO
```

`-O` overwrites the output file. Use `-o` only when intentionally appending.
Every direct crawl also retains its complete structured log at
`logs/<spider>/<UTC timestamp>_<process id>.jsonl`.

For a bounded test:

```powershell
scrapy crawl WRC_IE `
  -a start_date=01-01-2008 `
  -a end_date=31-03-2008 `
  -O wrc_test.jsonl `
  -s LOG_LEVEL=INFO `
  -s CLOSESPIDER_ITEMCOUNT=100
```

Several requests may already be in flight, so a limit of 100 can produce
slightly more than 100 items before the spider stops.

## Profile a spider

The opt-in crawl profiler runs any registered source or spider and writes a
timestamped JSON report. Persistence is disabled unless
`--with-persistence` is supplied, so profiling does not modify MongoDB or
MinIO by default.

Run a bounded profile through the source registry:

```powershell
python -m benchmarks.crawl_profiler `
  --source wrc_ie `
  -a start_date=01-01-2008 `
  -a end_date=31-01-2008 `
  --require-clean-git
```

Using `--source wrc_ie` applies the WRC settings registered in
`config/sources.json`. Use `--spider WRC_IE` to address the spider
directly. Repeat `-a NAME=VALUE` for spider arguments and `-s NAME=VALUE`
for temporary settings being evaluated; explicit `-s` values override both
the source profile and global defaults.

The current WRC profile was selected from a complete January 2008 partition.
With per-domain concurrency 6, a 0.1-second minimum delay, and AutoThrottle
target concurrency 3, the observed run scraped 150 of 151 discovered records
in 57.8 seconds (approximately 155.7 documents/minute), with zero request
failures, retries, blocked responses, or unexplained missing records. The one
extraction failure was a known empty duplicate landing page. These results
justify the WRC override only; they are not global defaults and should be
revalidated periodically and under representative conditions.

The matching structured event stream is retained under
`logs/profiles/<profile name>.jsonl` and its path is printed when the run
starts.

Each report under `reports/crawl_profiles/` records:

- Every spider argument, including `start_date` and `end_date`.
- The final effective concurrency, throttling, timeout and retry settings.
- The Git commit, branch, dirty-state flag, and tracked-diff fingerprint.
- Python, platform, CPU, direct dependency versions, and a complete installed-distribution snapshot with its own digest.
- SHA-256 fingerprints for the source registry, site configuration, and
  `requirements.txt`.
- Duration, scraped documents per minute and response-byte volume.
- Response-latency minimum, mean, p50, p95, p99 and maximum.
- HTTP status counts and rates, including `403`, `429` and combined `5xx`.
- Retry attempts, successful retry chains, exhausted chains and recovery rate.
- Expected, scraped, missing, dropped and extraction-failure counts.

Credential-like command arguments are represented by a SHA-256 fingerprint,
not written in plaintext. Use `--require-clean-git` for a formal benchmark;
it refuses to run when local code differs from the recorded commit. Omit it
while experimenting if you deliberately want to measure uncommitted code.

This makes the benchmark configuration reconstructable and auditable, but it
does not promise byte-for-byte replay of a live website. Exact replay would
also require archiving every HTTP response, which would add storage I/O and
change the performance being measured. The report states
`response_replay.available=false` explicitly.

When `--max-items` stops a sampled crawl, the report leaves
`unexplained_missing` unevaluated because the crawl was intentionally
incomplete. Use an uncapped representative partition to assess completeness.

The deterministic profiler calculations and failure handling can be tested
without contacting a website:

```powershell
python -m unittest discover -s tests -v
```

The request-failure tests inject retryable timeouts and final HTTP/timeout
failures. They verify that three configured retries create three replacement
requests, the next failure is marked exhausted, and final failures preserve
their URL, HTTP status when available, exact reason, and body/month
reconciliation counters. The timeout test is simulated and does not actually
wait 30 seconds.

The persistence tests inject one temporary MinIO upload failure and one
temporary MongoDB upsert failure. They verify recovery, retry counters, the
bounded exponential delay sequence, and that deterministic failures are not
retried. No live storage service is contacted.

To run only these tests:

```powershell
python -m unittest discover -s tests -p "test_request_failures.py" -v
```

Live profiles are operational measurements rather than normal CI tests:
external websites and network conditions can change between runs.

## Run with Dagster

Dagster requires persistence because the assets communicate through MongoDB
and MinIO. In `.env`, set:

```env
PERSISTENCE_ENABLED=true
```

Start the storage containers, then start Dagster:

```powershell
docker compose up -d
New-Item -ItemType Directory -Force .dagster
$env:DAGSTER_HOME = (Resolve-Path .dagster).Path
dagster dev -m kedra_scraper.definitions
```

Open `http://127.0.0.1:3000`. Select the `document_pipeline_job`, choose a
`source`/`date` partition, and materialise it. Dagster enforces
`raw_documents -> scraped_documents` for the same partition.

The included schedule launches the latest completed month for every source in
the registry. Its cron expression and timezone come from `.env`; enable the
schedule in the Dagster UI when ready.

Dagster always forces the spider subprocess to `SCRAPE_MODE=ingestion`, so text
extraction happens in the separate `scraped_documents` asset rather than in
the spider subprocess.

## Partitioning

Direct Scrapy runs split their requested interval according to
`SCRAPE_PARTITION_MONTHS`. A value of `1` creates calendar-month partitions:

```text
2024-01-01 to 2024-01-31
2024-02-01 to 2024-02-29
2024-03-01 to 2024-03-31
```

Every item receives the first date of its partition as `partition_date`.

Dagster uses an explicit two-dimensional partition key:

- `source`: a key from `config/sources.json`
- `date`: the first day of a calendar month

That lets different source/month combinations run independently and, when
executor capacity allows, concurrently.

## WRC document discovery

A WRC result may point to an HTML document, directly to a PDF, or to an HTML
wrapper containing the actual PDF download. Wrapper PDFs are selected with:

```css
div.related-item a.download::attr(href)
```

Using the scoped `.download` selector prevents unrelated site-wide PDFs, such
as the Cookie Policy, from being mistaken for case documents.

PDF text extraction uses `pypdf`. Image-only scanned PDFs are marked as
scraping failures until OCR support is added.

## Shared schemas

The raw ingestion item contains discovery metadata and the original response
bytes:

| Field | Type | Meaning |
| --- | --- | --- |
| `title` | `str` | Combined `identifier_description`, falling back to whichever value exists |
| `published_date` | `str` | Normalised publication date |
| `partition_date` | `str` | First date of the crawl partition |
| `identifier` | `str \| None` | Source identifier when available |
| `source` | `str` | Canonical source website |
| `category` | `str` | Source collection/category |
| `source_format` | `"html" \| "pdf"` | Original document format |
| `doc_url` | `str` | Final HTML or PDF URL |
| `landing_url` | `str` | Stable result URL used for identity |
| `raw_content` | `bytes` | Original bytes consumed by the MinIO pipeline |
| `description` | `str \| None` | Source description when available |
| `source_metadata` | `dict[str, Any]` | Extra source-specific metadata |

The scraped schema adds:

| Field | Type | Meaning |
| --- | --- | --- |
| `content` | `str` | Extracted HTML/PDF text |
| `content_hash` | `str` | SHA-256 of the scraped text encoded as UTF-8 |

`source_metadata` lets future spiders retain useful fields such as judges,
legislation, hearing dates, or court names without changing the common fields.

## Persistence and idempotency

The Scrapy item pipelines execute in order:

1. `MinioPipeline` builds `record_key`, stores original bytes, and attaches the
   blob metadata.
2. `MongoPipeline` upserts the record and its MinIO reference.

The stable identity is:

```text
record_key = SHA256(source + "|" + landing_url)
```

Identifiers are not database keys because sources can omit or duplicate them.
MongoDB has a unique index on `record_key`.

MinIO object names are deterministic:

```text
# Stable current object: overwritten when the source bytes or format change
documents/{record_key}/current
```

Reruns are safe:

- Unchanged MinIO bytes are not uploaded again.
- The stable `record_key`, derived from `source + landing_url`, identifies a
  replacement as the same logical document.
- When that object's stored SHA-256 differs from the newly downloaded bytes,
  MinIO writes to the same object key and replaces the older bytes.
- The content SHA-256 is metadata for change detection, not part of the object
  name; using it in the name would retain a separate historical object.
- MongoDB upserts rather than inserting a duplicate.
- MongoDB's `blob.object_key` points to the single current MinIO object.
- Legacy `blob_history` fields are removed on the next MongoDB upsert.
- Raw records become `pending` only when new or when their blob changes.
- `scraped_documents` skips a completed record when its scraped blob hash still
  matches the current raw blob.
- Changed originals clear stale scraped fields and are scraped again.
- `last_seen_at`, `ingested_at`, `scraped_at`, and `content_updated_at` preserve
  lifecycle timing.

## Retries and failure handling

Recovery operates at three scopes:

- Scrapy retries eligible individual HTTP requests according to
  `SCRAPY_RETRY_TIMES` and `SCRAPY_DOWNLOAD_TIMEOUT`.
- The persistence pipelines retry only transient MinIO/MongoDB operations.
  The default two retries wait 0.5 then 1 second; the delay doubles up to the
  configured maximum. Validation, authentication, and other deterministic
  errors are not retried.
- Dagster retries an entire failed source/month asset according to
  `DAGSTER_CRAWL_MAX_RETRIES` and
  `DAGSTER_CRAWL_RETRY_DELAY_SECONDS`, with exponential backoff.

The raw asset reads the spider's JSON crawl summary and rejects a partition
when its close reason, final request failures, persistence failures, or
unexplained missing count exceed configured limits. Because storage is
idempotent, retrying the whole partition is safe.

Persistence retries are also idempotent: MinIO repeats the same stable object
key and MongoDB repeats the same `record_key` upsert. Every attempt, recovery,
and exhausted retry chain is counted in the crawl statistics and written to
the structured log.

Operational scraping failures such as unavailable MongoDB/MinIO fail the
Dagster asset and are retried. A deterministic per-document extraction error
is stored on that record as `scraping_status="failed"` with `scraping_error`;
the rest of the partition continues. Set
`DAGSTER_FAIL_ON_SCRAPING_ERRORS=true` to mark that partition failed after all
documents have been attempted.

Subprocess output is captured so legal-document text does not flood the
Dagster UI. The complete Scrapy event stream is retained at
`logs/dagster/<source>/<partition>/raw_documents_<run id>.jsonl`, and that
path appears in the Dagster asset metadata. On failure, only the configured
subprocess tail is also copied into Dagster's own event log.

## Structured logging and reconciliation

Scrapy log records are emitted and retained as one JSON object per line.
Crawl-specific events include queryable fields such as `partition_date`,
`body`, `identifier`, `url`, `status_code`, `error_type`, and
`reason`.

The standard Python logging system is used because Scrapy and Dagster already
integrate with it directly. A rotating file handler caps each active file at
`SCRAPY_LOG_MAX_BYTES` (25 MB by default) and retains
`SCRAPY_LOG_BACKUP_COUNT` rotated files (5 by default). Set
`SCRAPY_LOG_DIR` to relocate all generated logs, or
`SCRAPY_STRUCTURED_LOG_PATH` to force a path for a standalone crawl.
Runtime files below `logs/` are ignored by Git.

Every body/month pair tracks:

- `found`: result cards discovered
- `succeeded`: items that completed all enabled item pipelines
- `failed`: incomplete results, exhausted downloads, extraction failures,
  or dropped items
- `request_failures`: document or search requests that exhausted retries

The final crawl summary contains a `body_partitions` array and emits one
`body_partition_summary` event per pair. This makes it possible to reconcile
found, successful, and failed documents without reading human-oriented log
messages.

## Verified WRC crawl

A complete direct test over `01-01-2008` through `31-03-2008` produced:

- 420 discovered records
- 419 scraped items
- 159 HTML documents
- 260 PDF documents
- 0 final request failures
- 1 empty duplicate landing page for `PW18/2007`
- 0 unexplained missing records

WRC exposes two different landing URLs named `PW18/2007`.
`pw18_20071.html` contains a downloadable PDF and succeeds;
`pw18_2007.html` contains neither document content nor a PDF download. This is
why the URL-based `record_key` is safer than the source identifier.

## Planned work

- Replace deprecated `FormRequest.from_response()` usage with `form2request`.
- Populate more source-specific metadata.
- Add OCR for scanned PDFs.
- Add automated parser, MinIO, MongoDB, and Dagster integration tests.
- Add a production secret manager for deployed credentials.
