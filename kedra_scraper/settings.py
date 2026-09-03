import os

from dotenv import load_dotenv


load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


# Scrapy settings for kedra_scraper project
BOT_NAME = "kedra_scraper"

SPIDER_MODULES = ["kedra_scraper.spiders"]
NEWSPIDER_MODULE = "kedra_scraper.spiders"

ADDONS = {}

USER_AGENT = "KedraScraper/1.0"

# Keep request summaries and warnings, but do not print every item's full content.
LOG_LEVEL = "INFO"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Concurrency and throttling settings
CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 0.1

COOKIES_ENABLED = False
TELNETCONSOLE_ENABLED = False

DEFAULT_REQUEST_HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "*/*;q=0.8"
    ),
    "Accept-Language": "en",
}

FEED_EXPORT_ENCODING = "utf-8"

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.5
AUTOTHROTTLE_MAX_DELAY = 30
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0
AUTOTHROTTLE_DEBUG = False

RETRY_TIMES = 3
DOWNLOAD_TIMEOUT = 30

# The MinIO stage also removes internal raw bytes before optional feed export.
ITEM_PIPELINES = {
    "kedra_scraper.pipelines.MinioPipeline": 300,
    "kedra_scraper.pipelines.MongoPipeline": 400,
}

# Persistence is opt-in so JSON test crawls work without local services.
PERSISTENCE_ENABLED = _env_bool("PERSISTENCE_ENABLED", False)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "kedra")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "documents")
MONGO_SERVER_SELECTION_TIMEOUT_MS = int(
    os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000")
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_SECURE = _env_bool("MINIO_SECURE", False)
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "kedra-documents")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "documents")

# Optional project components remain disabled until needed.
# SPIDER_MIDDLEWARES = {}
# DOWNLOADER_MIDDLEWARES = {}
# EXTENSIONS = {}

# HTTP caching remains disabled by default.
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = "httpcache"
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"
