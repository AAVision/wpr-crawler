import os
from dotenv import load_dotenv

load_dotenv()

BOT_NAME = "wr_scraper"
SPIDER_MODULES = ["scrapy_project.spiders"]
NEWSPIDER_MODULE = "scrapy_project.spiders"

# ============================================================
# HYBRID DOWNLOAD STRATEGY
# ============================================================
# The WRC search page loads results via JavaScript, so we NEED
# Playwright for those pages. Detail pages are static HTML, so
# we use scrapy-impersonate (curl_cffi) for speed + anti-detection.
#
# How it works:
# - Requests with meta["playwright"] = True → rendered by Playwright
# - All other requests → handled by scrapy-impersonate (fast HTTP)
# ============================================================
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "args": [
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-sandbox",
    ],
}

# Increase timeouts to handle slow server responses/Cloudflare validation
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60000
DOWNLOAD_TIMEOUT = 60

# Let scrapy-impersonate set the correct User-Agent for the
# impersonated browser automatically. We override per-request.
USER_AGENT = None

# ============================================================
# Anti-Detection: Rate Limiting & Human-like Behavior
# ============================================================
ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", 4))
CONCURRENT_REQUESTS_PER_DOMAIN = 2
DOWNLOAD_DELAY = float(os.getenv("REQUEST_DELAY", 3.0))

# Randomize delay between 50%-150% of DOWNLOAD_DELAY to look human
RANDOMIZE_DOWNLOAD_DELAY = True

# AutoThrottle dynamically adjusts speed based on server response times
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 3.0
AUTOTHROTTLE_MAX_DELAY = 20.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.5
AUTOTHROTTLE_DEBUG = False

# Retry on Cloudflare-specific codes
RETRY_ENABLED = True
RETRY_TIMES = int(os.getenv("RETRY_TIMES", 5))
RETRY_HTTP_CODES = [403, 429, 500, 502, 503, 504, 408, 520, 521, 522, 523, 524]

DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", 60))

# ============================================================
# Browser Impersonation Profiles (for non-Playwright requests)
# ============================================================
IMPERSONATE_BROWSERS = [
    "chrome124",
    "chrome123",
    "chrome120",
    "chrome119",
    "safari17_0",
    "safari15_5",
    "edge101",
]

# User agent strings matching the impersonated browsers
USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.47",
]

# ============================================================
# Middlewares
# ============================================================
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "scrapy_project.middlewares.BrowserImpersonationMiddleware": 100,
    "scrapy_project.middlewares.CloudflareRetryMiddleware": 500,
}

SPIDER_MIDDLEWARES = {
    "scrapy_project.middlewares.ErrorLoggingMiddleware": 543,
}

ITEM_PIPELINES = {
    "scrapy_project.pipelines.HashCalculationPipeline": 100,
    "scrapy_project.pipelines.MinIOStoragePipeline": 200,
    "scrapy_project.pipelines.MongoDBPipeline": 300,
}

# ============================================================
# Cookie & Session Persistence
# ============================================================
COOKIES_ENABLED = True
COOKIES_DEBUG = False

# ============================================================
# Default Headers
# ============================================================
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

# Set DEPTH_LIMIT to 0 for unlimited pagination depth (necessary for many pages of results)
DEPTH_LIMIT = 0
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"

# ============================================================
# Proxy Support (optional but recommended for production)
# ============================================================
PROXY_ENABLED = os.getenv("PROXY_ENABLED", "false").lower() == "true"
if PROXY_ENABLED:
    DOWNLOADER_MIDDLEWARES[
        "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware"
    ] = 750
    PROXY_URL = os.getenv("PROXY_URL")
