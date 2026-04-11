import scrapy
import re
import random
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from urllib.parse import urlencode

from scrapy.http import HtmlResponse
from scrapy_project.items import DecisionItem
from scrapy_project.settings import IMPERSONATE_BROWSERS
from scrapy_playwright.page import PageMethod
from utils.logging_utils import setup_logging
from utils.metadata import BODIES, get_body_name

# Standardized logging
logger = setup_logging(__name__)

class WorkplaceRelationsSpider(scrapy.Spider):
    """
    Highly concurrent WRC Spider using Scrapy benchmarks.
    Optimizes partition discovery and document extraction.
    """
    name = "wr_spider"
    allowed_domains = ["workplacerelations.ie"]
    base_url = "https://www.workplacerelations.ie"
    search_base = f"{base_url}/en/search/"

    def __init__(self, start_date=None, end_date=None, body_id="all", partition_months=1, **kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date
        self.body_id = str(body_id)
        self.partition_months = int(partition_months or 1)

    def start_requests(self):
        """
        Entry point: Multi-month concurrent discovery.
        Each monthly partition is fired as a separate high-priority request.
        """
        for p_start, p_end in self._date_partitions():
            yield self._request_search_page(p_start, p_end)

    def _date_partitions(self):
        """Generator for monthly chunks."""
        try:
            current = datetime.strptime(self.start_date, "%Y-%m-%d").replace(day=1)
            end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        except (ValueError, TypeError):
            current = (datetime.now() - timedelta(days=30)).replace(day=1)
            end_dt = datetime.now()

        while current <= end_dt:
            p_start = current
            p_end = min(current + relativedelta(months=self.partition_months) - timedelta(days=1), end_dt)
            yield p_start, p_end
            current += relativedelta(months=self.partition_months)

    def _request_search_page(self, start, end):
        """Builds the search query URL and Scrapy Request."""
        target_ids = [b["id"] for b in BODIES] if self.body_id.lower() == "all" else [self.body_id]
        
        query = [("decisions", "1"), ("from", start.strftime("%d/%m/%Y")), ("to", end.strftime("%d/%m/%Y"))]
        for bid in target_ids:
            query.append(("body", bid.strip()))

        return scrapy.Request(
            url=f"{self.search_base}?{urlencode(query)}",
            callback=self.parse_search_results,
            meta={
                "body_id": ",".join(target_ids),
                "partition": start.strftime("%Y-%m"),
                "playwright": True,
                "playwright_page_goto_kwargs": {"wait_until": "domcontentloaded"},
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "li.each-item, .results-count", timeout=60000),
                    # Optimization: Block resources we don't need
                    PageMethod("route", "**/*.{png,jpg,jpeg,svg,gif,woff,woff2,css}", lambda route: route.abort()),
                ],
            },
            priority=100, # Discovery is priority #1
            errback=self.handle_error
        )

    def parse_search_results(self, response: HtmlResponse):
        """Parses the search result list and depth-first pagination."""
        self.crawler.stats.inc_value("wrc/pages_crawled")
        
        # Immediate Pagination Follow
        next_btn = response.css("a.next::attr(href)").get()
        if next_btn:
            yield response.follow(
                next_btn,
                callback=self.parse_search_results,
                meta=response.meta,
                priority=110, # Deep discovery gets top priority
                dont_filter=True
            )

        # Dispatch Case Extractions
        for row in response.css("li.each-item"):
            href = row.css("a.btn-primary::attr(href)").get() or row.css("h2.title a::attr(href)").get()
            if href:
                self.crawler.stats.inc_value("wrc/links_discovered")
                yield scrapy.Request(
                    url=response.urljoin(href),
                    callback=self.parse_decision_detail,
                    meta={
                        "identifier": row.css("h2.title a::text").get("").strip(),
                        "date_text": row.css("span.date::text").get("").strip(),
                        "body_id": response.meta.get("body_id"),
                        "partition": response.meta.get("partition"),
                        "impersonate": random.choice(IMPERSONATE_BROWSERS),
                    },
                    priority=50, # Extraction is priority #2
                    errback=self.handle_error
                )

    def parse_decision_detail(self, response: HtmlResponse):
        """Final case data extraction and document dispatch."""
        identifier = response.meta.get("identifier") or self._utils_extract_id(response)
        date = self._utils_resolve_date(response)
        doc_url, doc_type = self._utils_get_doc_meta(response)

        item = DecisionItem()
        item.update({
            "identifier": identifier,
            "title": identifier,
            "date": date.strftime("%Y-%m-%d") if date else None,
            "body": get_body_name(response.meta.get("body_id")),
            "link_to_doc": doc_url or response.url,
            "partition_date": date.strftime("%Y-%m") if date else response.meta.get("partition"),
            "source_url": response.url,
            "scraped_at": datetime.now().isoformat(),
            "document_type": doc_type or "html"
        })

        if doc_url and doc_type in ("pdf", "doc", "docx"):
            yield scrapy.Request(
                url=doc_url,
                callback=self.save_binary,
                meta={"item": item, "impersonate": random.choice(IMPERSONATE_BROWSERS)},
                errback=self.handle_download_error,
                dont_filter=True
            )
        else:
            item["document_content"] = response.body
            self.crawler.stats.inc_value("wrc/records_scraped")
            yield item

    def save_binary(self, response):
        """Final handler for non-HTML artifacts."""
        item = response.meta["item"]
        item["document_content"] = response.body
        self.crawler.stats.inc_value("wrc/records_scraped")
        yield item

    # --- Utility Helpers ---

    def _utils_resolve_date(self, response):
        dt = response.meta.get("date_text") or self._utils_regex_date(response)
        parsed = self._utils_parse_ds(dt)
        return parsed or self._utils_url_date(response.url)

    def _utils_regex_date(self, response):
        patterns = [r"(\d{2}/\d{2}/\d{4})", r"(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})"]
        for p in patterns:
            match = re.search(p, response.text[:20000], re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def _utils_parse_ds(self, ds):
        if not ds:
            return None
        for f in ["%d/%m/%Y", "%d %B %Y", "%d-%m-%Y", "%Y-%m-%d"]:
            try:
                return datetime.strptime(ds.strip(), f)
            except ValueError: 
                continue
        return None

    def _utils_url_date(self, url):
        m = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,"july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
        match = re.search(r"/cases/(\d{4})/([^/]+)/", url)
        return datetime(int(match.group(1)), m.get(match.group(2).lower(), 1), 1) if match else None

    def _utils_get_doc_meta(self, response):
        main = response.css("#main") or response
        for ext in [".pdf", ".doc", ".docx"]:
            link = main.css(f'a[href$="{ext}"]::attr(href), a[href$="{ext.upper()}"]::attr(href)').get()
            if link and "cookie" not in link.lower():
                return response.urljoin(link), ext.lstrip(".").lower()
        return None, "html"

    def _utils_extract_id(self, response):
        return response.url.rstrip("/").split("/")[-1].upper()

    def handle_error(self, f):
        logger.error(f"Request failed: {f.request.url}")

    def handle_download_error(self, f):
        item = f.request.meta.get("item")
        if item:
            item["document_type"] = "html"
            yield item

    def closed(self, reason):
        s = self.crawler.stats.get_stats()
        scraped_count = s.get("wrc/records_scraped", 0)
        summary = {
            "found": s.get("wrc/links_discovered", 0),
            "scraped": scraped_count,
            "pages": s.get("wrc/pages_crawled", 0),
            "reason": reason
        }
        logger.info(f"Spider closed. Summary: {json.dumps(summary)}")
        logger.info(f"TOTAL SCRAPED URLS: {scraped_count}")
