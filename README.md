# Kedra Scraping Pipeline

A Scrapy-based pipeline for collecting legal documents and normalising them into a shared document schema. The first implemented source is the Irish Workplace Relations Commission (WRC).

## Current status

The WRC spider currently supports:

- Required `start_date` and `end_date` crawl arguments.
- Monthly partitions between the requested dates.
- Separate searches across all four WRC Body categories.
- Pagination through every search-results page.
- HTML case-document extraction.
- Detection and extraction of PDF-backed cases.
- Automatic retries, timeouts and AutoThrottle.
- Request and extraction failure accounting.
- JSON Lines export for local testing.
- A shared item schema with flexible source-specific metadata.
- Deterministic SHA-256 hashes of extracted document content.

MongoDB/GridFS persistence is planned but is not enabled yet. The current item pipeline is still a pass-through stub.

## Supported WRC categories

- Employment Appeals Tribunal
- Equality Tribunal
- Labour Court
- Workplace Relations Commission

## Installation

Python 3.11 or later is recommended.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Run commands from the repository root, where `scrapy.cfg` is located.

## Running the WRC spider

The spider accepts dates in either `DD-MM-YYYY` or `YYYY-MM-DD` format.

```powershell
scrapy crawl WRC_IE `
  -a start_date=01-01-2008 `
  -a end_date=31-03-2008 `
  -O wrc_old_test.jsonl `
  -s LOG_LEVEL=INFO
```

`-O` overwrites the output file. Use `-o` only when intentionally appending to an existing feed.

For a bounded test crawl:

```powershell
scrapy crawl WRC_IE `
  -a start_date=01-01-2008 `
  -a end_date=31-03-2008 `
  -O wrc_test.jsonl `
  -s LOG_LEVEL=INFO `
  -s CLOSESPIDER_ITEMCOUNT=100
```

Because several requests may already be in flight, `CLOSESPIDER_ITEMCOUNT=100` can produce slightly more than 100 items before shutdown completes.

## Partitioning

The requested interval is split into monthly partitions. For example:

```text
start_date: 01-01-2024
end_date:   31-03-2024

partitions:
2024-01-01 to 2024-01-31
2024-02-01 to 2024-02-29
2024-03-01 to 2024-03-31
```

Each record receives the first date of its monthly partition as `partition_date`.

For every monthly partition, the spider submits one search per WRC Body category. These searches can run concurrently while remaining subject to the configured per-domain concurrency and AutoThrottle limits.

## Document flow

A search result may point directly to an HTML document, directly to a PDF, or to an HTML wrapper containing a PDF download.

```text
search page
  -> case URL
     -> HTML content -> yield HTML item
     -> a.download PDF link -> request PDF -> yield PDF item
```

Older WRC records can use HTML wrapper pages. The spider specifically selects:

```css
div.related-item a.download::attr(href)
```

This prevents unrelated site-wide PDFs, such as the Cookie Policy, from being mistaken for case documents.

PDF text is currently using `pypdf`. Image-only scanned PDFs currently require future OCR support.

## Scraped item schema

Every source-specific spider maps its data into the same item:

| Field | Type | Meaning |
| --- | --- | --- |
| `title` | `str` | Combined `identifier_description`, falling back to whichever component exists |
| `published_date` | `str` | Normalised publication date in `YYYY-MM-DD` form |
| `partition_date` | `str` | First date of the monthly crawl partition |
| `content` | `str` | Extracted HTML or PDF text |
| `content_hash` | `str` | SHA-256 hash of the extracted content |
| `identifier` | `str \| None` | Source-provided document identifier, when available |
| `source` | `str` | Source website |
| `category` | `str` | Source collection or WRC Body category |
| `source_format` | `"html" \| "pdf"` | Format from which content was extracted |
| `doc_url` | `str` | Final HTML or PDF document URL |
| `description` | `str \| None` | Source-provided record description |
| `source_metadata` | `dict[str, Any]` | Additional useful fields unique to a source |

For example:

```json
{
  "title": "TE54/2007_CASE DESCRIPTION",
  "published_date": "2008-02-01",
  "partition_date": "2008-02-01",
  "content": "Extracted legal document text...",
  "content_hash": "64-character SHA-256 hexadecimal digest",
  "identifier": "TE54/2007",
  "source": "https://www.workplacerelations.ie",
  "category": "Employment Appeals Tribunal",
  "source_format": "pdf",
  "doc_url": "https://www.workplacerelations.ie/en/eat_import/...pdf",
  "description": "CASE DESCRIPTION",
  "source_metadata": {}
}
```

`source_metadata` defaults to an empty dictionary, so existing spiders remain compatible. Future sources can preserve fields such as judges, legislation, hearing dates or court names without changing the common top-level contract.

`content_hash` is calculated from the exact extracted `content` encoded as UTF-8. It represents the logical text of both HTML and PDF documents. A separate raw-file hash can be added later when original files are stored with GridFS.

## Crawl statistics and failures

The spider records statistics including:

- `documents/expected`
- `documents/scheduled`
- `documents/html_extracted`
- `documents/pdf_extracted`
- `documents/request_failed`
- `errors/html_parsing`
- `errors/html_empty`
- `errors/pdf_parsing`
- `errors/pdf_empty`
- `errors/document_hashing`

The final reconciliation summary compares discovered search results with yielded items:

```text
CRAWL SUMMARY:
expected=420
scraped=419
request_failed=0
extraction_failed=1
missing=1
unexplained=0
```

`unexplained=0` means every missing item is accounted for by a logged failure. Search pages, pagination pages, HTML wrappers and `robots.txt` all count as crawled responses but do not necessarily produce items, so the response count is normally higher than the item count.

### Verified January-March 2008 test

A complete test over `01-01-2008` through `31-03-2008` produced:

- 420 discovered records
- 419 extracted items
- 159 HTML documents
- 260 PDF documents
- 0 final request failures
- 1 empty duplicate landing page for `PW18/2007`; the separate PDF-backed `PW18/2007` URL was extracted successfully
- 0 unexplained missing records

WRC exposes two distinct landing-page URLs named `PW18/2007`. `pw18_20071.html` is an HTML wrapper with a downloadable PDF and is extracted successfully. `pw18_2007.html` contains neither HTML document content nor a PDF download link, so only that empty duplicate URL is recorded as an extraction failure instead of yielding an empty item.

A source identifier must therefore not be assumed to be unique. The future persistence layer should use a stable source URL or generated record key for idempotency rather than relying only on `source + identifier`.

## Request settings

The project currently uses:

- `ROBOTSTXT_OBEY = True`
- `CONCURRENT_REQUESTS_PER_DOMAIN = 4`
- `DOWNLOAD_DELAY = 0.1`
- `AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0`
- `DOWNLOAD_TIMEOUT = 30`
- `RETRY_TIMES = 3`
- `LOG_LEVEL = "INFO"`

AutoThrottle adjusts request timing based on observed server latency. The configured concurrency values are ceilings rather than a guarantee that every slot is continuously occupied.

## Planned work

- Replace the deprecated `FormRequest.from_response()` flow with `form2request`.
- Implement MongoDB metadata persistence.
- Store original document files using GridFS.
- Add idempotent upserts and stable record keys.
- Populate source-specific metadata.
- Add OCR handling for scanned PDFs.
- Add automated parser and integration tests.
