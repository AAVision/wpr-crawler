import random
import json
import time
import logging
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message
from scrapy_project.settings import USER_AGENT_LIST, IMPERSONATE_BROWSERS

logger = logging.getLogger(__name__)


def _log_structured(event: str, data: dict):
    """Emit a structured JSON log entry."""
    from datetime import datetime

    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "component": "middleware",
        "event": event,
        **data,
    }
    logger.info(json.dumps(log_entry))


class BrowserImpersonationMiddleware:
    """Rotate browser TLS fingerprints + matching User-Agent on every request.

    This is the core Cloudflare bypass mechanism. scrapy-impersonate uses
    curl_cffi to mimic real browser TLS handshakes (JA3/JA4 fingerprints),
    which is the primary way Cloudflare detects scraping bots.

    Each request gets:
    - A random browser impersonation profile (e.g., chrome124, safari17_0)
    - A matching User-Agent header
    - Consistent Sec-CH-UA headers that match the impersonated browser
    """

    def process_request(self, request, spider=None):
        # Pick a random browser profile to impersonate
        browser = random.choice(IMPERSONATE_BROWSERS)
        request.meta["impersonate"] = browser

        # Set matching User-Agent
        ua = random.choice(USER_AGENT_LIST)
        request.headers["User-Agent"] = ua

        # Add Sec-CH-UA headers matching the browser identity
        if "chrome" in browser.lower():
            version = "".join(filter(str.isdigit, browser))
            request.headers["Sec-CH-UA"] = (
                f'"Chromium";v="{version}", '
                f'"Google Chrome";v="{version}", '
                f'"Not-A.Brand";v="99"'
            )
            request.headers["Sec-CH-UA-Mobile"] = "?0"
            request.headers["Sec-CH-UA-Platform"] = '"Windows"'
        elif "safari" in browser.lower():
            request.headers["Sec-CH-UA-Mobile"] = "?0"
            request.headers["Sec-CH-UA-Platform"] = '"macOS"'
        elif "edge" in browser.lower():
            version = "".join(filter(str.isdigit, browser))
            request.headers["Sec-CH-UA"] = (
                f'"Microsoft Edge";v="{version}", '
                f'"Chromium";v="{version}", '
                f'"Not-A.Brand";v="99"'
            )
            request.headers["Sec-CH-UA-Mobile"] = "?0"
            request.headers["Sec-CH-UA-Platform"] = '"Windows"'

        # Add referer for non-initial requests to look more natural
        if "search" in (request.url or "") and request.meta.get("page_number", 1) > 1:
            request.headers["Referer"] = request.url.split("&pageNumber")[0]


class CloudflareRetryMiddleware(RetryMiddleware):
    """Handle Cloudflare-specific responses with intelligent retry logic.

    Cloudflare responses to detect and handle:
    - 403 Forbidden: May be a Cloudflare challenge page
    - 429 Too Many Requests: Rate limiting
    - 503 Service Unavailable: "Checking your browser..." interstitial
    - 520-524: Cloudflare edge errors
    """

    # Cloudflare challenge page indicators
    CF_CHALLENGE_MARKERS = [
        b"cf-browser-verification",
        b"cf_chl_opt",
        b"Checking your browser",
        b"cf-challenge-running",
        b"_cf_chl_tk",
        b"just a moment",
        b"Attention Required",
        b"cloudflare",
    ]

    def process_response(self, request, response, spider=None):
        # --- 429 Too Many Requests ---
        if response.status == 429:
            retry_after = response.headers.get("Retry-After", b"30")
            try:
                wait_time = int(retry_after)
            except (ValueError, TypeError):
                wait_time = 30

            _log_structured(
                "rate_limited",
                {
                    "url": request.url,
                    "status": 429,
                    "retry_after_seconds": wait_time,
                    "body": getattr(spider, "body_name", "unknown"),
                },
            )

            # Wait with jitter before retrying
            jitter = random.uniform(0.5, 1.5)
            time.sleep(min(wait_time * jitter, 90))

            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response

        # --- 403 / 503: Possible Cloudflare challenge ---
        if response.status in (403, 503):
            if self._is_cloudflare_challenge(response):
                _log_structured(
                    "cloudflare_challenge",
                    {
                        "url": request.url,
                        "status": response.status,
                        "body": getattr(spider, "body_name", "unknown"),
                        "action": "retry_with_delay",
                    },
                )

                # Wait longer for Cloudflare challenges — the site may
                # ease up after a delay
                time.sleep(random.uniform(10, 25))

                # Switch to a different browser profile on retry
                request.meta["impersonate"] = random.choice(IMPERSONATE_BROWSERS)
                request.headers["User-Agent"] = random.choice(USER_AGENT_LIST)

                reason = f"Cloudflare challenge ({response.status})"
                return self._retry(request, reason, spider) or response

        # --- 520-524: Cloudflare edge errors ---
        if 520 <= response.status <= 524:
            _log_structured(
                "cloudflare_edge_error",
                {
                    "url": request.url,
                    "status": response.status,
                },
            )
            time.sleep(random.uniform(5, 15))
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response

        return super().process_response(request, response)

    def _is_cloudflare_challenge(self, response) -> bool:
        """Detect if the response is a Cloudflare challenge page."""
        body = response.body[:5000].lower()
        return any(marker.lower() in body for marker in self.CF_CHALLENGE_MARKERS)


class ErrorLoggingMiddleware:
    """Log spider exceptions with structured JSON output."""

    def process_spider_exception(self, response, exception, spider=None):
        _log_structured(
            "spider_exception",
            {
                "url": response.url,
                "error": str(exception),
                "error_type": type(exception).__name__,
                "body": getattr(spider, "body_name", "unknown"),
                "partition_date": getattr(spider, "partition_date", "unknown"),
            },
        )
        return None
