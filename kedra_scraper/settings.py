import os

from dotenv import load_dotenv

from kedra_scraper.source_registry import DEFAULT_SOURCE_REGISTRY_PATH
from kedra_scraper.utils import env_bool, env_int, env_float


load_dotenv()

# Scrapy project wiring. Runtime behaviour is configured below.
BOT_NAME = "kedra_scraper"
SPIDER_MODULES = ["kedra_scraper.spiders"]
NEWSPIDER_MODULE = "kedra_scraper.spiders"
ADDONS = {}

# Source and partition configuration.
SOURCE_REGISTRY_PATH = os.getenv(
    "SOURCE_REGISTRY_PATH",
    DEFAULT_SOURCE_REGISTRY_PATH,
)

SCRAPE_START_DATE = os.getenv("SCRAPE_START_DATE")
SCRAPE_END_DATE = os.getenv("SCRAPE_END_DATE")
SCRAPE_PARTITION_MONTHS = env_int(
    "SCRAPE_PARTITION_MONTHS",
    1,
    minimum=1,
)
SCRAPE_MODE = os.getenv("SCRAPE_MODE", "full").strip().lower()
if SCRAPE_MODE not in {"full", "ingestion"}:
    raise ValueError("SCRAPE_MODE must be either 'full' or 'ingestion'")
CRAWL_SUMMARY_PATH = os.getenv("CRAWL_SUMMARY_PATH")
CRAWL_PROFILE_PATH = os.getenv("CRAWL_PROFILE_PATH")

# Durable structured logs. An explicit path is supplied by Dagster and the
# profiler; standalone Scrapy runs derive a unique path below this directory.
STRUCTURED_LOG_DIR = os.getenv("SCRAPY_LOG_DIR", "logs")
STRUCTURED_LOG_PATH = os.getenv("SCRAPY_STRUCTURED_LOG_PATH")
STRUCTURED_LOG_MAX_BYTES = env_int(
    "SCRAPY_LOG_MAX_BYTES",
    25_000_000,
    minimum=1,
)
STRUCTURED_LOG_BACKUP_COUNT = env_int(
    "SCRAPY_LOG_BACKUP_COUNT",
    5,
    minimum=0,
)

# Request identity, output and protocol behaviour.
USER_AGENT = os.getenv("SCRAPY_USER_AGENT", "KedraScraper/1.0")
LOG_LEVEL = os.getenv("SCRAPY_LOG_LEVEL", "INFO").upper()
ROBOTSTXT_OBEY = env_bool("SCRAPY_ROBOTSTXT_OBEY", True)
COOKIES_ENABLED = env_bool("SCRAPY_COOKIES_ENABLED", False)
TELNETCONSOLE_ENABLED = env_bool(
    "SCRAPY_TELNETCONSOLE_ENABLED",
    False,
)
FEED_EXPORT_ENCODING = os.getenv("SCRAPY_FEED_EXPORT_ENCODING", "utf-8")

DEFAULT_REQUEST_HEADERS = {
    "Accept": os.getenv(
        "SCRAPY_ACCEPT_HEADER",
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    ),
    "Accept-Language": os.getenv("SCRAPY_ACCEPT_LANGUAGE", "en"),
}

# Conservative cross-site fallbacks. A spider may load benchmarked overrides
# from config/source_registry.json. Do not raise these global defaults based on one
# website's benchmark because every host has different capacity and limits.
# Explicit command-line settings (-s NAME=VALUE) still take highest priority.
# Concurrency, throttling and resilience.
CONCURRENT_REQUESTS = env_int(
    "SCRAPY_CONCURRENT_REQUESTS",
    16,
    minimum=1,
)
CONCURRENT_REQUESTS_PER_DOMAIN = env_int(
    "SCRAPY_CONCURRENT_REQUESTS_PER_DOMAIN",
    2,
    minimum=1,
)
DOWNLOAD_DELAY = env_float(
    "SCRAPY_DOWNLOAD_DELAY",
    0.5,
    minimum=0.0,
)
RANDOMIZE_DOWNLOAD_DELAY = env_bool(
    "SCRAPY_RANDOMIZE_DOWNLOAD_DELAY",
    True,
)
DOWNLOAD_TIMEOUT = env_float(
    "SCRAPY_DOWNLOAD_TIMEOUT",
    30.0,
    minimum=0.001,
)
RETRY_TIMES = env_int("SCRAPY_RETRY_TIMES", 3, minimum=0)

AUTOTHROTTLE_ENABLED = env_bool(
    "SCRAPY_AUTOTHROTTLE_ENABLED",
    True,
)
AUTOTHROTTLE_START_DELAY = env_float(
    "SCRAPY_AUTOTHROTTLE_START_DELAY",
    0.5,
    minimum=0.0,
)
AUTOTHROTTLE_MAX_DELAY = env_float(
    "SCRAPY_AUTOTHROTTLE_MAX_DELAY",
    30.0,
    minimum=0.0,
)
AUTOTHROTTLE_TARGET_CONCURRENCY = env_float(
    "SCRAPY_AUTOTHROTTLE_TARGET_CONCURRENCY",
    1.0,
    minimum=0.001,
)
AUTOTHROTTLE_DEBUG = env_bool("SCRAPY_AUTOTHROTTLE_DEBUG", False)

# These numbers just decide the order of pipeline stages.
ITEM_PIPELINES = {
    "kedra_scraper.pipelines.MinioPipeline": 300,
    "kedra_scraper.pipelines.MongoPipeline": 400,
}

# Persistence is opt-in so JSON test crawls work without local services.
PERSISTENCE_ENABLED = env_bool("PERSISTENCE_ENABLED", False)
PERSISTENCE_RETRY_TIMES = env_int(
    "PERSISTENCE_RETRY_TIMES",
    2,
    minimum=0,
)
PERSISTENCE_RETRY_BASE_DELAY_SECONDS = env_float(
    "PERSISTENCE_RETRY_BASE_DELAY_SECONDS",
    0.5,
    minimum=0.0,
)
PERSISTENCE_RETRY_MAX_DELAY_SECONDS = env_float(
    "PERSISTENCE_RETRY_MAX_DELAY_SECONDS",
    5.0,
    minimum=0.0,
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "kedra")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "documents")
MONGO_SERVER_SELECTION_TIMEOUT_MS = env_int(
    "MONGO_SERVER_SELECTION_TIMEOUT_MS",
    5000,
    minimum=1,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_SECURE = env_bool("MINIO_SECURE", False)
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "kedra-documents")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "documents")

# Structured JSON logging is always enabled. The profiler remains dormant
# unless CRAWL_PROFILE_PATH is supplied.
EXTENSIONS = {
    "kedra_scraper.utils.structured_logging.StructuredLoggingExtension": 100,
    "benchmarks.extensions.CrawlProfilerExtension": 500,
}

# Optional middleware components remain disabled until needed.
# SPIDER_MIDDLEWARES = {}
# DOWNLOADER_MIDDLEWARES = {}

# HTTP caching remains disabled by default.
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = "httpcache"
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"
