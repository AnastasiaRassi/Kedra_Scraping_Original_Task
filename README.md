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

The Dagster definitions are source-agnostic. `config/source_registry.json` maps a
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

Configuration is intentionally split by scope:

- `.env` contains application runtime values read by the Python assets,
  spiders, persistence clients, and Docker Compose.
- `config/source_registry.json` contains per-source spider selection, benchmarked
  request-rate overrides, and the path to each site's own config file
  (`SITE_CONFIG_PATH`).
- Each site's own config file, named after its spider (e.g.
  `kedra_scraper/spiders/ie/wrc_cfg.json`), contains that site's selectors and
  form details.
- `config/dagster.yaml` contains Dagster instance behavior that applies across
  runs, currently the maximum number of simultaneously active partition runs.

All connection strings, storage locations, partition settings, crawl tuning,
and Dagster retry settings are therefore configurable without changing Python
source. No local credentials belong in source code.

| Area | Main variables |
| --- | --- |
| Storage switch | `PERSISTENCE_ENABLED` |
| Storage retries | `PERSISTENCE_RETRY_TIMES`, `PERSISTENCE_RETRY_BASE_DELAY_SECONDS`, `PERSISTENCE_RETRY_MAX_DELAY_SECONDS` |
| MongoDB | `MONGO_URI`, `MONGO_DATABASE`, `MONGO_COLLECTION`, `MONGO_SERVER_SELECTION_TIMEOUT_MS` |
| MinIO | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_SECURE`, `MINIO_BUCKET`, `MINIO_PREFIX` |
| Docker | Image, container, bind-host, port, credential, and data-path variables in `.env.example` |
| Direct crawl period | `SCRAPE_START_DATE`, `SCRAPE_END_DATE`, `SCRAPE_PARTITION_MONTHS` |
| Source rules | `SOURCE_REGISTRY_PATH` (per-source `SITE_CONFIG_PATH` lives in the registry, not `.env`) |
| Request behavior | `SCRAPY_ROBOTSTXT_OBEY`, `SCRAPY_COOKIES_ENABLED`, `SCRAPY_DOWNLOAD_TIMEOUT`, `SCRAPY_RETRY_TIMES` |
| Structured logs | `SCRAPY_LOG_DIR`, `SCRAPY_STRUCTURED_LOG_PATH`, `SCRAPY_LOG_MAX_BYTES`, `SCRAPY_LOG_BACKUP_COUNT` |
| Concurrency | `SCRAPY_CONCURRENT_REQUESTS`, `SCRAPY_CONCURRENT_REQUESTS_PER_DOMAIN`, `SCRAPY_DOWNLOAD_DELAY` |
| AutoThrottle | `SCRAPY_AUTOTHROTTLE_ENABLED`, `SCRAPY_AUTOTHROTTLE_START_DELAY`, `SCRAPY_AUTOTHROTTLE_MAX_DELAY`, `SCRAPY_AUTOTHROTTLE_TARGET_CONCURRENCY` |
| Dagster run queue | `config/dagster.yaml` (`concurrency.runs.max_concurrent_runs`) |
| Dagster partitions | `DAGSTER_PARTITION_START_DATE`, `DAGSTER_PARTITION_END_DATE`, `DAGSTER_PARTITION_TIMEZONE`, `DAGSTER_PARTITION_END_OFFSET` |
| Dagster retries | `DAGSTER_CRAWL_MAX_RETRIES`, `DAGSTER_CRAWL_RETRY_DELAY_SECONDS`, `DAGSTER_CRAWL_TIMEOUT_SECONDS` |
| Dagster load profiling | `DAGSTER_BENCHMARK_TIMEOUT_SECONDS` |
| Dagster validation | `DAGSTER_MAX_REQUEST_FAILURES`, `DAGSTER_MAX_PERSISTENCE_ERRORS`, `DAGSTER_MAX_UNEXPLAINED_MISSING`, `DAGSTER_FAIL_ON_SCRAPING_ERRORS` |
| Dagster log tail | `DAGSTER_CRAWL_LOG_LEVEL`, `DAGSTER_SUBPROCESS_LOG_TAIL_LINES`, `DAGSTER_SUBPROCESS_LOG_TAIL_CHARACTERS` |
| Stored extraction errors | `SCRAPING_ERROR_MAX_CHARS` |
| Schedule | `DAGSTER_SCHEDULE_CRON` |

All supported variables and development defaults are listed in
`.env.example`. Numeric and Boolean values are validated early.

### Source registry

`config/source_registry.json` is the orchestration registry. Its keys become Dagster's
`source` partitions:

```json
{
  "sources": {
    "wrc_ie": {
      "spider": "WRC_IE",
      "source": "https://www.workplacerelations.ie",
      "spider_settings": {
        "SITE_CONFIG_PATH": "kedra_scraper/spiders/ie/wrc_cfg.json",
        "CONCURRENT_REQUESTS_PER_DOMAIN": "6",
        "DOWNLOAD_DELAY": "0.1",
        "AUTOTHROTTLE_ENABLED": "true",
        "AUTOTHROTTLE_TARGET_CONCURRENCY": "3.0"
      },
      "site_config_setting": "SITE_CONFIG_PATH"
    }
  }
}
```

The `spider_settings` values override the conservative global request-rate
defaults for that source. Direct Scrapy crawls and Dagster ingestion load the
same overrides; the load profiler submits that Dagster job rather than
reimplementing the settings. Explicit `-s NAME=VALUE` values can still override
settings during isolated direct-crawl experiments.

Global rate settings must remain conservative because a benchmark against one
website says nothing about another website's capacity or blocking policy. Add
rate overrides for a new source only after benchmarking that source on a
representative complete partition.

To add another website, implement a source spider that accepts
`start_date`/`end_date`, yields the shared raw item in ingestion mode, and
writes the shared crawl-summary counters. Then add its entry to the registry.
The Dagster assets do not need website-specific branches.

## Run a direct Scrapy test

A direct crawl only ingests: the spider downloads each file and yields a
`KedraRawDocumentItem` with the raw bytes. Text extraction always happens
separately, in the `scraped_documents` Dagster asset (see below).

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

## Profile Dagster multi-partition load

The profiler exercises the production-shaped path: it submits the real
`document_pipeline_job` once for every selected source/month and lets
Dagster's run queue decide how many partitions overlap. The former
single-spider benchmark is no longer the primary performance test because it
could not expose cross-partition contention in Dagster, MongoDB, MinIO, or the
source website.

Start storage and Dagster first:

```powershell
docker compose up -d
.\scripts\start_dagster.ps1
```

Keep that terminal open. In a second activated terminal, profile at least two
monthly partitions:

```powershell
python -m benchmarks.crawl_profiler `
  --source wrc_ie `
  --start-partition 2026-01-01 `
  --end-partition 2026-03-01 `
  --expect-max-concurrent-runs 1 `
  --require-clean-git
```

Both partition arguments are inclusive and must use `YYYY-MM-01`. The
default safety limit is 12 partitions; raise it deliberately with
`--max-partitions N` for a larger load test. The command uses
`http://127.0.0.1:3000` by default. Repository names are inferred when the
deployment exposes only one matching job; otherwise pass
`--repository-location` and `--repository`.

The active limit is read from `$DAGSTER_HOME/dagster.yaml` (or
`.dagster/dagster.yaml`). `--expect-max-concurrent-runs` prevents measuring
the wrong queue configuration. To compare limits, change the tracked
`config/dagster.yaml`, restart Dagster through the launcher, and repeat the
same source and partition range. The profiler does not silently reconfigure a
running Dagster instance.

Reports are written under `reports/dagster_load_profiles/`. Each report
contains:

- Requested partitions, Dagster run IDs, final statuses, queue time, execution
  time, and observed peak active runs.
- End-to-end wall time and aggregate records/minute across all partitions.
- Expected, ingested, newly scraped, unchanged, missing, and failed document
  counts, plus found/succeeded/failed reconciliation for every source body and
  month.
- Request/response counts, bytes, HTTP status totals, and cross-partition
  latency summaries.
- Scrapy retry attempts, recovered and exhausted retry chains, plus observed
  Dagster raw-asset retries.
- Persistence counters for MinIO and MongoDB.
- Exact structured failure events, including URL, status code, error type, and
  reason when available.
- Git revision, dirty state, Python/dependency versions, source settings, and
  source/Dagster configuration fingerprints.

Each Dagster run now writes a crawl profile beside its structured log and a
summary for the downstream scraping asset:

```text
logs/dagster/<source>/<partition>/raw_documents_<run-id>.jsonl
logs/dagster/<source>/<partition>/raw_documents_<run-id>.profile.json
logs/dagster/<source>/<partition>/scraped_documents_<run-id>.summary.json
```

A successful command exits with code 0. It exits with code 1 and still writes
the report when any partition fails, the profiler times out, a profile/summary
is missing, final request or persistence failures occur, unexplained raw
documents are missing, or document scraping fails.

`newly_scraped` and `unchanged` remain separate. A warm idempotent rerun can
be faster because unchanged blobs skip extraction; the report must not present
that as equivalent to a cold first ingestion. Compare like-for-like runs.

Use `--require-clean-git` for evidence you will report. Omit it only while
intentionally measuring uncommitted code. The report reconstructs code,
configuration, inputs, and runtime versions, but does not archive live HTTP
responses, so byte-for-byte replay of a changing website is not promised.

The deterministic profile calculations and failure handling can be tested
without contacting the website:

```powershell
python -m unittest discover -s tests -v
```

The request-failure tests inject retryable and final HTTP failures. Persistence
tests inject temporary MinIO and MongoDB failures. Load-profiler tests verify
month selection, Dagster multi-partition tags, cross-run aggregation, exact
failure-event retention, and instance-concurrency parsing. Live load profiles
remain operational measurements because the website and local machine load can
change between runs.

## Run with Dagster

Dagster requires persistence because the assets communicate through MongoDB
and MinIO. In `.env`, set:

```env
PERSISTENCE_ENABLED=true
```

Start the storage containers, then use the included launcher:

```powershell
docker compose up -d
.\scripts\start_dagster.ps1
```

The launcher copies the tracked `config/dagster.yaml` into
`$DAGSTER_HOME/dagster.yaml`, then starts the webserver from the repository
root so Dagster also loads `.env`. The instance file directly sets the
maximum number of simultaneous partition runs. The `DAGSTER_*` application
variables remain in `.env` because `kedra_scraper/definitions.py` and
`kedra_scraper/utils/dagster_utils.py` read them directly.

Open `http://127.0.0.1:3000`. Select the `document_pipeline_job`, choose a
`source`/`date` partition, and materialise it. Dagster enforces
`raw_documents -> scraped_documents` for the same partition.

The included schedule launches the latest completed month for every source in
the registry. Its cron expression and timezone come from `.env`; enable the
schedule in the Dagster UI when ready.

The spider subprocess only ever ingests raw bytes; text extraction always
happens in the separate `scraped_documents` asset rather than in the spider
subprocess.

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

- `source`: a key from `config/source_registry.json`
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
# Flat files: overwritten when the source bytes change
documents/{record_key}.pdf
documents/{record_key}.html
```

Reruns are safe:

- Unchanged MinIO bytes are not uploaded again.
- The stable `record_key`, derived from `source + landing_url`, identifies a
  replacement as the same logical document.
- When that object's stored SHA-256 differs from the newly downloaded bytes,
  MinIO writes to the same object key and replaces the older bytes.
- The old `documents/{record_key}/current` layout is deleted after the flat
  replacement is stored successfully, so its virtual MinIO folder disappears
  when that document is crawled again.
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
