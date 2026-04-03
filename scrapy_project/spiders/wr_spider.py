import scrapy
from scrapy.http import HtmlResponse
from urllib.parse import urljoin, urlencode
import re
import os
import json
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from scrapy_project.items import DecisionItem
from scrapy_project.settings import IMPERSONATE_BROWSERS
from scrapy_playwright.page import PageMethod
from utils.logging_utils import setup_logging

# Centralized Logging
logger = setup_logging(__name__)

class WorkplaceRelationsSpider(scrapy.Spider):
    name = "wr_spider"
    allowed_domains = ["workplacerelations.ie"]
    base_url = "https://www.workplacerelations.ie"
    search_base = f"{base_url}/en/search/"

    # Correct body IDs from the actual website
    BODY_MAP = {
        "1": "Employment Appeals Tribunal",
        "2": "Equality Tribunal",
        "3": "Labour Court",
        "15376": "Workplace Relations Commission",
    }

    def __init__(
        self,
        start_date=None,
        end_date=None,
        body_id=None,
        body_name=None,
        partition_date=None,
        partition_months=1,
        url_list_file=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date
        self.body_id = str(body_id) if body_id else "all"
        self.body_name = body_name or self.BODY_MAP.get(self.body_id, self.body_id)
        self.partition_date = partition_date or datetime.now().strftime("%Y-%m")
        self.partition_months = int(partition_months or 1)
        self.url_list_file = url_list_file

        # Stats tracking
        self._stats = {
            "records_found": 0,
            "records_scraped": 0,
            "records_skipped_duplicate": 0,
            "downloads_failed": [],
            "pages_crawled": 0,
        }

    def start_requests(self):
        """Phase 2: Use discovered URLs or fallback to internal discovery."""
        if self.url_list_file and os.path.exists(self.url_list_file):
            with open(self.url_list_file, "r") as f:
                urls = json.load(f)
            
            self.logger.info(f"Extracting {len(urls)} URLs from pre-discovered list.")
            for url in urls:
                self._stats["records_found"] += 1
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_decision_detail,
                    meta={
                        "impersonate": random.choice(IMPERSONATE_BROWSERS), 
                        "partition_date": self.partition_date,
                        "body_id": self.body_id
                    },
                    errback=self.handle_error,
                )
            return

        # Fallback to internal discovery
        bid = self.body_id if self.body_id and self.body_id.lower() != "all" else ",".join(self.BODY_MAP.keys())
        # Use more robust body params (body=1&body=2...)
        bid_list = bid.split(",")
        
        # Monthly partitions
        try:
            start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        except:
            # Safe fallbacks if parsing fails
            start_dt = datetime.now() - timedelta(days=30)
            end_dt = datetime.now()

        current = start_dt.replace(day=1)
        while current <= end_dt:
            p_start = current
            p_end = min(current + relativedelta(months=self.partition_months) - timedelta(days=1), end_dt)
            
            # Construct complex params list for urlencode(doseq=True)
            params = [
                ("decisions", "1"),
                ("from", p_start.strftime("%d/%m/%Y")),
                ("to", p_end.strftime("%d/%m/%Y")),
            ]
            for b in bid_list:
                params.append(("body", b.strip()))
                
            url = f"{self.search_base}?{urlencode(params)}"
            yield scrapy.Request(
                url=url,
                callback=self.parse_search_results,
                meta={
                    "page_number": 1,
                    "body_id": bid,
                    "partition_date": current.strftime("%Y-%m"),
                    "playwright": True,
                    "playwright_page_methods": [PageMethod("wait_for_selector", "li.each-item, .results-count", timeout=50000)],
                },
                errback=self.handle_error,
            )
            current += relativedelta(months=self.partition_months)

    def parse_search_results(self, response: HtmlResponse):
        """Parse search result listing page."""
        self._stats["pages_crawled"] += 1
        page_num = response.meta.get("page_number", 1)

        # Pagination
        next_link = response.css("a.next::attr(href)").get()
        if next_link:
            yield scrapy.Request(
                url=response.urljoin(next_link),
                callback=self.parse_search_results,
                meta={
                    **response.meta,
                    "page_number": page_num + 1,
                },
                errback=self.handle_error,
            )

        # Extraction from page
        for item in response.css("li.each-item"):
            href = item.css("a.btn-primary::attr(href)").get() or item.css("h2.title a::attr(href)").get()
            if href:
                full_url = urljoin(self.base_url, href)
                self._stats["records_found"] += 1
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_decision_detail,
                    meta={
                        "identifier": item.css("h2.title a::text").get("").strip(),
                        "date_text": item.css("span.date::text").get("").strip(),
                        "body_id": response.meta.get("body_id"),
                        "impersonate": random.choice(IMPERSONATE_BROWSERS),
                        "partition_date": response.meta.get("partition_date")
                    },
                    errback=self.handle_error,
                )

    def parse_decision_detail(self, response: HtmlResponse):
        """Parse case detail page with fallback and URL date extraction."""
        identifier = response.meta.get("identifier") or self._extract_identifier(response)
        date_text = response.meta.get("date_text") or self._extract_date(response)
        date = self._parse_date(date_text)
        
        # If still no date, extract from URL (e.g. /cases/2001/january/)
        if not date:
            date = self._extract_date_from_url(response.url)
            if date:
                logger.info(f"Salvagged date from URL for {identifier}: {date.strftime('%Y-%m-%d')}")

        doc_url, doc_type = self._get_document_url(response)
        
        # Body name mapping
        bid = response.meta.get("body_id") or self.body_id
        if bid and "," in str(bid):
             body_name = "Workplace Relations"
        else:
             body_name = self.BODY_MAP.get(str(bid), "Workplace Relations")

        item = DecisionItem()
        item["identifier"] = identifier
        item["title"] = identifier
        item["date"] = date.strftime("%Y-%m-%d") if date else None
        item["body"] = body_name
        item["link_to_doc"] = doc_url or response.url
        item["partition_date"] = date.strftime("%Y-%m") if date else response.meta.get("partition_date")
        item["source_url"] = response.url
        item["scraped_at"] = datetime.now().isoformat()
        item["document_type"] = doc_type or "html"

        if doc_url and doc_type in ("pdf", "doc", "docx"):
            yield scrapy.Request(
                url=doc_url, callback=self.save_document,
                meta={"item": item, "impersonate": random.choice(IMPERSONATE_BROWSERS)},
                errback=self.handle_download_error, dont_filter=True
            )
        else:
            item["document_content"] = response.body
            self._stats["records_scraped"] += 1
            yield item

    def save_document(self, response: HtmlResponse):
        item = response.meta["item"]
        item["document_content"] = response.body
        self._stats["records_scraped"] += 1
        yield item

    def _extract_identifier(self, response):
        return response.url.rstrip("/").split("/")[-1].upper()

    def _extract_date(self, response):
        patterns = [
            r"(\d{2}/\d{2}/\d{4})", 
            r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})",
            r"(\d{2}-\d{2}-\d{4})"
        ]
        text_chunk = response.text[:20000] # Increased range
        for p in patterns:
            match = re.search(p, text_chunk, re.IGNORECASE)
            if match: return match.group(1)
        return None

    def _extract_date_from_url(self, url):
        # Format: /cases/YYYY/monthname/
        months = {
            "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
            "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
        }
        match = re.search(r"/cases/(\d{4})/([^/]+)/", url)
        if match:
            year = int(match.group(1))
            month_name = match.group(2).lower()
            month = months.get(month_name, 1)
            return datetime(year, month, 1)
        return None

    def _parse_date(self, date_str):
        if not date_str: return None
        for fmt in ["%d/%m/%Y", "%d %B %Y", "%d-%m-%Y", "%Y-%m-%d"]:
            try: return datetime.strptime(date_str.strip(), fmt)
            except ValueError: continue
        return None

    def _get_document_url(self, response):
        main = response.css("#main") or response
        for ext in [".pdf", ".PDF", ".doc", ".docx"]:
            link = main.css(f'a[href$="{ext}"]::attr(href)').get()
            if link and "cookie" not in link.lower():
                return urljoin(self.base_url, link), ext.lstrip(".").lower()
        return None, "html"

    def handle_error(self, failure):
        logger.error(f"Request failed: {failure.request.url} - {failure.value}")

    def handle_download_error(self, failure):
        item = failure.request.meta.get("item")
        if item:
            item["document_type"] = "html"
            yield item

    def closed(self, reason):
        logger.info(json.dumps({
            "event": "run_summary", "records_found": self._stats["records_found"],
            "records_scraped": self._stats["records_scraped"], "reason": reason
        }))
