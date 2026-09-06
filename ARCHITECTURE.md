# Kedra Scraping Pipeline Architecture

This document provides a high-level overview of the architectural decisions and design patterns used in the Kedra Scraping Pipeline.

## Date Partitioning Strategy

The pipeline operates on **calendar-month date partitions**. This specific partition size was chosen because it was explicitly requested, and it serves as the ideal temporal boundary given the average frequency and volume of document publications per month. A monthly grouping ensures that partitions are small enough to retry quickly upon failure without prolonged re-processing, yet large enough to avoid excessive orchestration overhead and partition fragmentation within Dagster.

## Retries and Rate Limiting

We implement robust mechanisms to ensure polite scraping and highly resilient operations at three distinct layers:

1. **Rate Limiting**: Scrapy is configured defensively by default to respect external system stability. We utilize `AutoThrottle` to dynamically adjust request delays against website capacity and set strict asynchronous concurrent-request limits per domain (`CONCURRENT_REQUESTS_PER_DOMAIN`) via `config/source_registry.json`.
2. **Micro-Retries (Scrapy level)**: Scrapy automatically intercepts and handles transient HTTP failures (e.g., 502, 503, 504 gateway timeouts), obeying `SCRAPY_DOWNLOAD_TIMEOUT` and standard scrapy retry parameters without failing the pipeline.
3. **Macro-Retries (Dagster level)**: Dagster orchestrates macro-level recovery limits (`TASK_RETRY_POLICY`). If a calendar-month partition completely fails (e.g., due to catastrophic unexplained missing items or persistent container errors), Dagster utilizes exponential backoff to handle the rerun automatically across all interconnected assets smoothly.

## Deduplication Strategy (Idempotency)

Data integrity requires that the pipeline can be aggressively rerun without yielding duplicate items. We achieve safety across storage and database boundaries through deterministic **upserting**:

- **Stable Identity**: Each discovered item is assigned a permanent `record_key` hash, calculated deterministically as `SHA256(source + "|" + landing_url)`. We use the URL rather than internal site strings since URLs are universally stable.
- **MongoDB Upserting**: When an item completes the `MongoPipeline`, MongoDB executes an **upsert** operation based on a unique index applied to the `record_key`. This guarantees no duplicated records pollute the schema.
- **MinIO Collision Protection**: On the blob storage side, MinIO leverages identical object paths (e.g., `documents/{record_key}.html`). Subsequent run passes read the fresh scraped data blob and compute a SHA256 bytes-hash; if unmodified, the upload step simply skips. If differing, it deterministically overwrites the `current` state file.

## Scaling to 50+ Sources

While the current JSON-registry array and local orchestration setup excel at predictability and low complexity, a transition to 50+ distinctly unique legal sources warrants addressing several new structural and operational scaling bottlenecks:

1. **Centralized Configuration Database**: Flat files (`config/source_registry.json` and disparate JSON configs) become unwieldy at a large scale. The static web registry should be migrated into a relational database model (e.g., PostgreSQL). This unlocks real-time operational flexibility (enabling administrators to pause problematic sources, triage failing configurations, and tune spider-specific rate limits at runtime) without redeploying application code.
2. **Generic Spiders & Rule-Based Extractors**: Maintaining 50+ unique bespoke Scrapy spider classes is highly susceptible to maintenance rot. We would adopt generic template spiders directed entirely by configurable site-specific extraction rulesets (e.g., dynamically injecting XPath endpoints or RSS parsing paradigms), restricting custom web spider usage exclusively for sites utilizing sophisticated JavaScript-heavy DOMs.
3. **Advanced Anti-Bot Networking**: At larger scale, scraping defense capabilities skyrocket in severity. A 50-source mandate requires integrating continuous comprehensive external proxy rotation (dynamically migrating between residential and datacenter connections), CAPTCHA solving integrations, and potentially browser automation layers (such as Playwright API pools) for more demanding anti-scraping walls. 
4. **Distributed Execution and Anomaly Detection**: A localized Docker-compose Dagster limits execution capabilities. The architecture would necessitate horizontal executor scaling (via Kubernetes/K8s pods via Dagster Helm charts) along with rigorous automated anomaly detection on parsed data vectors—ensuring failing pipeline elements raise alerting rather than demanding manual inspection across hundreds of runs.
