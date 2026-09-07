import os
import sys
import tempfile
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Definitions,
    Failure,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    RunRequest,
    SkipReason,
    StaticPartitionsDefinition,
    asset,
    define_asset_job,
    schedule,
)
from dotenv import load_dotenv

from kedra_scraper.source_registry import (
    DEFAULT_SOURCE_REGISTRY_PATH,
    load_source_registry,
)
from kedra_scraper.text_extraction import scrape_partition
from kedra_scraper.utils import env_bool, env_float, env_int, write_crawl_summary
from kedra_scraper.utils.dagster_utils import (
    TASK_RETRY_POLICY,
    dagster_scraping_summary_path,
    dagster_structured_log_path,
    ingestion_violations,
    log_subprocess_tail,
    partition_context,
    read_summary,
    require_persistence,
    run_scrapy,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SOURCE_REGISTRY = load_source_registry(
    os.getenv("SOURCE_REGISTRY_PATH", DEFAULT_SOURCE_REGISTRY_PATH)
)

# (Ask Emilio)
# Hardcoding the start date defaults to 2000-01-01 to avoid checking oldest available0 Website
# documents on every run. If historical data older than our start date appears unexpectedly, 
# standard static partitions won't catch it. Do we use Dagster Dynamic Partitions & Sensors 
# or is the edge case too unlikely
MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(
    start_date=os.getenv("DAGSTER_PARTITION_START_DATE", "2000-01-01"),
    end_date=os.getenv("DAGSTER_PARTITION_END_DATE") or None,
    timezone=os.getenv("DAGSTER_PARTITION_TIMEZONE", "Europe/Dublin"),
    end_offset=env_int("DAGSTER_PARTITION_END_OFFSET", 0),
)

# Because we choose both dates & sources as we partition: 
SOURCE_MONTH_PARTITIONS = MultiPartitionsDefinition(
    {
        "date": MONTHLY_PARTITIONS,
        "source": StaticPartitionsDefinition(sorted(SOURCE_REGISTRY)),
    }
)


@asset(
    name="raw_documents",
    group_name="document_ingestion",
    description=(
        "Ingest original source documents and metadata into MinIO and MongoDB."
    ),
    partitions_def=SOURCE_MONTH_PARTITIONS,
    retry_policy=TASK_RETRY_POLICY,
)
def raw_documents(context: AssetExecutionContext) -> MaterializeResult:
    """Run one configured source spider for one calendar month."""
    require_persistence()
    source_config, partition_date, start_date, end_date = partition_context(
        context,
        MONTHLY_PARTITIONS,
    )
    timeout_seconds = env_float(
        "DAGSTER_CRAWL_TIMEOUT_SECONDS",
        7200.0,
        minimum=1.0,
    )
    log_level = os.getenv("DAGSTER_CRAWL_LOG_LEVEL", "INFO").strip().upper()
    structured_log_path = dagster_structured_log_path(
        source_config.key,
        partition_date,
        context.run_id,
    )
    crawl_profile_path = structured_log_path.with_suffix(".profile.json")

    with tempfile.TemporaryDirectory(prefix="kedra-ingestion-") as directory:
        summary_path = Path(directory) / "crawl-summary.json"
        # the following defines the command which we would run by terminal to dictate start, end dates 
        # and scrape mode etc ... 
        command = [
            sys.executable,
            "-m",
            "scrapy",
            "crawl",
            source_config.spider,
            "-a",
            f"start_date={start_date:%d-%m-%Y}",
            "-a",
            f"end_date={end_date:%d-%m-%Y}",
            "-s",
            "SCRAPE_MODE=ingestion",
            "-s",
            f"CRAWL_SUMMARY_PATH={summary_path}",
            "-s",
            f"LOG_LEVEL={log_level}",
            "-s",
            f"STRUCTURED_LOG_PATH={structured_log_path}",
            "-s",
            f"CRAWL_PROFILE_PATH={crawl_profile_path}",
            "-s",
            f"CRAWL_PROFILE_SOURCE={source_config.key}",
        ]
        for name, value in source_config.spider_settings.items():
            command.extend(("-s", f"{name}={value}"))

        context.log.info(
            "Starting source=%s ingestion for %s through %s",
            source_config.key,
            start_date.isoformat(),
            end_date.isoformat(),
        )
        context.log.info("Scrapy JSON log: %s", structured_log_path)
        context.log.info("Scrapy crawl profile: %s", crawl_profile_path)
        result = run_scrapy(
            context, # to stream errors here 
            command, # to run the command
            timeout_seconds=timeout_seconds,
        )
        summary = read_summary(summary_path)
        violations = ingestion_violations(summary)

        if violations:
            log_subprocess_tail(context, "stderr", result.stderr)
            raise Failure(
                description="; ".join(violations),
                metadata={
                    **summary,
                    "scrapy_log_path": str(structured_log_path),
                    "crawl_profile_path": str(crawl_profile_path),
                },
                allow_retries=True,
            )

        context.log.info(
            "Raw ingestion completed: expected=%s stored=%s",
            summary.get("expected", 0),
            summary.get("scraped", 0),
        )
        return MaterializeResult(
            metadata={
                "source": source_config.key,
                "partition_start": start_date.isoformat(),
                "partition_end": end_date.isoformat(),
                "scrapy_log_path": str(structured_log_path),
                "crawl_profile_path": str(crawl_profile_path),
                **summary,
            }
        )


@asset(
    name="scraped_documents",
    group_name="document_scraping",
    description=(
        "Extract and normalize text from raw MinIO documents into MongoDB."
    ),
    deps=[raw_documents],
    partitions_def=SOURCE_MONTH_PARTITIONS,
    retry_policy=TASK_RETRY_POLICY,
)
def scraped_documents(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape the same source/month only after raw ingestion succeeds."""
    require_persistence()
    source_config, partition_date, start_date, end_date = partition_context(
        context,
        MONTHLY_PARTITIONS,
    )

    context.log.info(
        "Starting source=%s scraping for partition=%s",
        source_config.key,
        partition_date,
    )
    summary_path = dagster_scraping_summary_path(
        source_config.key,
        partition_date,
        context.run_id,
    )
    try:
        summary = scrape_partition(source_config, partition_date)
    except Exception as exc:
        write_crawl_summary(
            str(summary_path),
            {
                "source": source_config.key,
                "partition_date": partition_date,
                "raw_documents": 0,
                "scraped": 0,
                "unchanged": 0,
                "scraping_failed": 0,
                "operational_failure": f"{type(exc).__name__}: {exc}",
            },
        )
        raise Failure(
            description=(
                f"Scraping failed operationally for source="
                f"{source_config.key}, partition={partition_date}: {exc}"
            ),
            allow_retries=True,
        ) from exc

    write_crawl_summary(
        str(summary_path),
        {
            "source": source_config.key,
            "partition_date": partition_date,
            **summary,
        },
    )

    failed = summary["scraping_failed"]
    if failed:
        context.log.warning(
            "Scraping completed with %s document extraction failure(s)",
            failed,
        )
        if env_bool("DAGSTER_FAIL_ON_SCRAPING_ERRORS", False):
            raise Failure(
                description=(
                    f"{failed} document(s) could not be scraped for "
                    f"source={source_config.key}, partition={partition_date}"
                ),
                metadata=summary,
                allow_retries=False,
            )

    context.log.info(
        "Scraping completed: scraped=%s unchanged=%s failed=%s",
        summary["scraped"],
        summary["unchanged"],
        failed,
    )
    return MaterializeResult(
        metadata={
            "source": source_config.key,
            "partition_start": start_date.isoformat(),
            "partition_end": end_date.isoformat(),
            "scraping_summary_path": str(summary_path),
            **summary,
        }
    )


document_pipeline_job = define_asset_job(
    name="document_pipeline_job", # Name of the pipeline in the Dagster U
    selection=AssetSelection.assets(raw_documents, scraped_documents),  #Tells Dagster to run both raw_documents (downloading) and scraped_documents (text parsing) in sequence.
    partitions_def=SOURCE_MONTH_PARTITIONS, # Applies the 2D grid (source + month) so I can run or backfill this job for one specific source and month at a time.
)


@schedule(
    job=document_pipeline_job,
    cron_schedule=os.getenv("DAGSTER_SCHEDULE_CRON", "0 2 1 * *"),
    execution_timezone=os.getenv(
        "DAGSTER_PARTITION_TIMEZONE",
        "Asia/Beirut",
    ),
)
def document_pipeline_schedule(context):
    """Launch the latest completed month once for every configured source."""
    date_key = MONTHLY_PARTITIONS.get_last_partition_key( # Because it can only run on completed months. I wonder if thats a smart approach.
    # Maybe it's an edge case: current month documents are skipped
        current_time=context.scheduled_execution_time
    )
    if date_key is None:
        return SkipReason("No completed monthly partition is available")

    return [
        RunRequest(
            run_key=f"{source_key}:{date_key}",
            partition_key=MultiPartitionKey(
                {"date": date_key, "source": source_key}
            ),
        )
        # launches automated run for all sources
        for source_key in sorted(SOURCE_REGISTRY)
    ]


defs = Definitions(
    assets=[raw_documents, scraped_documents],
    jobs=[document_pipeline_job],
    schedules=[document_pipeline_schedule],
)
