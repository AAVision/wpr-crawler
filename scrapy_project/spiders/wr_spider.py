import scrapy
from scrapy.http import HtmlResponse
from urllib.parse import urljoin, urlencode
import re
import json
import random
from datetime import datetime

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
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date
        self.body_id = body_id
        self.body_name = body_name or self.BODY_MAP.get(body_id, body_id)
        self.partition_date = partition_date or datetime.now().strftime("%Y-%m")

        # Stats tracking for structured logging
        self._stats = {
            "records_found": 0,
            "records_scraped": 0,
            "records_skipped_duplicate": 0,
            "downloads_failed": [],
            "pages_crawled": 0,
        }

        # Format dates for URL params (DD/MM/YYYY)
        if start_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            self.start_date_formatted = start_dt.strftime("%d/%m/%Y")
        else:
            self.start_date_formatted = None

        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            self.end_date_formatted = end_dt.strftime("%d/%m/%Y")
        else:
            self.end_date_formatted = None

    def start_requests(self):
        """Yield a single consolidated start request for all bodies if body_id is 'all'."""
        bid = self.body_id
        if not bid or bid.lower() == "all":
            # Use comma-separated list for maximum speed (combined pagination)
            bid = ",".join(self.BODY_MAP.keys())

        # If it's a list or 'all', we use 'All Bodies' as the placeholder name
        # Individual items will have their body parsed from their detail page
        log_name = self.body_name if "," not in str(bid) else "Combined Bodies"

        self._log_structured(
            "partition_start",
            {
                "partition_date": self.partition_date,
                "body_id": bid,
                "body_name": log_name,
                "start_date": self.start_date,
                "end_date": self.end_date,
            },
        )

        params = {"decisions": "1", "body": bid}
        if self.start_date_formatted:
            params["from"] = self.start_date_formatted
        if self.end_date_formatted:
            params["to"] = self.end_date_formatted

        url = f"{self.search_base}?{urlencode(params)}"
        print(url)
        print("=================================")

        yield scrapy.Request(
            url=url,
            callback=self.parse_search_results,
            meta={
                "page_number": 1,
                "body_id": bid,
                "body_name": log_name,
                "playwright": True,
                "playwright_include_page": False,
                "playwright_page_methods": [
                    PageMethod(
                        "wait_for_selector",
                        "li.each-item, .results-count",
                        timeout=30000,
                    ),
                ],
            },
            errback=self.handle_error,
            dont_filter=True,
        )

    def parse_search_results(self, response: HtmlResponse):
        """Parse search result listing page.

        Actual DOM structure (Updated 2026):
        <li class="each-item clearfix">
          <div class="row">
            <div class="col-sm-9">
              <h2 class="title" title="ADJ-00047801">
                <a href="/en/cases/...">ADJ-00047801</a>
              </h2>
            </div>
            <div class="col-sm-3">
              <span class="date">30/01/2025</span>
            </div>
          </div>
          <p class="description">Joanna Gurtman V Sport Ireland</p>
          <div class="row bottom-ref">
            <div class="col-sm-3 link">
              <a class="btn btn-primary" href="/en/cases/...">View Page</a>
            </div>
          </div>
        </li>
        """
        self._stats["pages_crawled"] += 1
        page_num = response.meta.get("page_number", 1)

        # 1. Pagination First: Ensure we follow the next page even if parsing this page fails
        next_link = response.css("a.next::attr(href)").get()
        if next_link:
            next_url = response.urljoin(next_link)
            print(next_url)
            print("=================================")
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_search_results,
                meta={
                    "page_number": page_num + 1,
                    "body_id": response.meta.get("body_id"),
                    "body_name": response.meta.get("body_name"),
                    "playwright": True,
                    "playwright_include_page": False,
                    "playwright_page_methods": [
                        PageMethod(
                            "wait_for_selector",
                            "li.each-item, .results-count",
                            timeout=30000,
                        ),
                    ],
                },
                errback=self.handle_error,
                dont_filter=True,
            )

        # 2. Extract total count for monitoring
        results_text = response.xpath('//text()[contains(., "Shows")]').re_first(
            r"Shows\s+\d+\s+to\s+\d+\s+of\s+([\d,]+)\s+results"
        )
        if results_text:
            total = int(results_text.replace(",", ""))
            if page_num == 1:
                self._log_structured(
                    "search_results_count",
                    {
                        "total_results": total,
                        "body": self.body_name,
                        "partition_date": self.partition_date,
                    },
                )

        # 3. Parse each result container: <li> with class "each-item"
        result_items = response.css("li.each-item")
        if not result_items:
            self._log_structured(
                "no_results_on_page",
                {
                    "page_number": page_num,
                    "body": self.body_name,
                    "url": response.url,
                },
            )
            return

        found_on_page = 0
        for item in result_items:
            try:
                # Robust link selection: try both known classes and fallback to any primary button link
                href = (
                    item.css("h2.title a::attr(href)").get()
                    or item.css("a.view-decision::attr(href)").get()
                    or item.css("a.btn-primary::attr(href)").get()
                    or item.css("a::attr(href)").get()
                )

                if not href or "/en/cases/" not in href:
                    continue
                full_url = urljoin(self.base_url, href)

                # Identifier from <h2> title
                identifier = item.css("h2.title a::text").get("").strip()
                if not identifier:
                    identifier = item.css("h2.title::attr(title)").get("").strip()

                # Clean identifier from en-dashes and weird whitespace
                identifier = identifier.replace(" – ", "-").replace(" - ", "-").strip()

                # Date from span.date
                date_text = item.css("span.date::text").get("").strip()

                # Description from p.description
                description = item.css("p.description::text").get("").strip()
                # Clean description (normalize spaces/tabs)
                description = " ".join(description.split())

                found_on_page += 1
                self._stats["records_found"] += 1

                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_decision_detail,
                    meta={
                        "identifier": identifier,
                        "title": identifier,
                        "date_text": date_text,
                        "description": description,
                        "search_url": response.url,
                        "impersonate": random.choice(IMPERSONATE_BROWSERS),
                    },
                    errback=self.handle_error,
                )
            except Exception as e:
                self._log_structured(
                    "item_parse_error",
                    {
                        "page_number": page_num,
                        "error": str(e),
                    },
                )

        self._log_structured(
            "page_parsed",
            {
                "page_number": page_num,
                "records_on_page": found_on_page,
                "body": self.body_name,
                "partition_date": self.partition_date,
            },
        )

    def parse_decision_detail(self, response: HtmlResponse):
        """Parse individual decision/case detail page and extract metadata."""
        # 1. Extract body from the <title> tag for correct attribution in combined searches
        # Format: "IDENTIFIER - BODY_NAME"
        page_title = response.css("title::text").get("").strip()
        body_from_title = None
        if " - " in page_title:
            body_from_title = page_title.split(" - ", 1)[-1].strip()

        identifier = response.meta.get("identifier") or self._extract_identifier(
            response
        )
        title = response.meta.get("title") or self._extract_title(response)
        description = response.meta.get("description") or self._extract_description(
            response
        )
        date_text = response.meta.get("date_text") or self._extract_date(response)
        date = self._parse_date(date_text)

        # Check for downloadable PDF/DOC documents
        document_url, document_type = self._get_document_url(response)

        item = DecisionItem()
        item["identifier"] = identifier
        item["title"] = title
        item["description"] = description
        item["date"] = date.isoformat() if date else None
        item["body"] = (
            body_from_title or response.meta.get("body_name") or self.body_name
        )
        item["link_to_doc"] = document_url or response.url
        item["partition_date"] = self.partition_date
        item["source_url"] = response.url
        item["scraped_at"] = datetime.now().isoformat()
        item["document_type"] = document_type or "html"

        if document_url and document_type in ("pdf", "doc", "docx"):
            # Download the binary document file
            yield scrapy.Request(
                url=document_url,
                callback=self.save_document,
                meta={
                    "item": item,
                    "impersonate": random.choice(IMPERSONATE_BROWSERS),
                },
                errback=self.handle_download_error,
            )
        else:
            # Store current HTML page as the document
            item["document_type"] = "html"
            item["document_content"] = response.body
            item["link_to_doc"] = response.url
            self._stats["records_scraped"] += 1
            yield item

    def save_document(self, response: HtmlResponse):
        """Callback for downloaded PDF/DOC documents."""
        item = response.meta["item"]
        item["document_content"] = response.body
        self._stats["records_scraped"] += 1
        yield item

    def _extract_identifier(self, response: HtmlResponse) -> str:
        """Extract case identifier from the detail page."""
        # First try the prominent heading pattern: "ADJ-00055934"
        h1_text = response.css("h1::text").get("")
        adj_match = re.search(
            r"(ADJ-\d+|DET-\d+|DEE-\d+|EDA-\d+|EET-\d+|"
            r"FET-\d+|MN-\d+|UD-\d+|RP-\d+|WT-\d+|"
            r"CD-\d+|CA-\d+|TED-\d+|DWT-\d+)",
            h1_text,
            re.IGNORECASE,
        )
        if adj_match:
            return adj_match.group(0).upper()

        # Try "Adjudication Reference:" field
        adj_ref = response.xpath(
            '//strong[contains(text(), "Adjudication Reference")]/following-sibling::text()'
        ).get()
        if adj_ref:
            return adj_ref.strip()

        # Fallback from any text on page
        patterns = [
            r"(ADJ-\d+)",
            r"(DET-\d+)",
            r"(DEE-\d+)",
            r"(CD/\d+/\d+)",
            r"(UD\d+/\d+)",
        ]
        text = response.text[:5000]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).upper()

        # Last resort: from URL
        return response.url.rstrip("/").split("/")[-1].replace(".html", "").upper()

    def _extract_title(self, response: HtmlResponse) -> str:
        """Extract title from detail page."""
        # Try h1 first (usually "ADJ-XXXXX")
        h1 = response.css("h1::text").get()
        if h1 and h1.strip():
            return h1.strip()

        # Try h2 (usually "ADJUDICATION OFFICER DECISION")
        h2 = response.css("h2::text").get()
        if h2 and h2.strip():
            return h2.strip()

        # Try page title
        title = response.css("title::text").get()
        if title:
            return title.strip()

        return "Untitled"

    def _extract_description(self, response: HtmlResponse) -> str:
        """Extract description — typically the parties involved."""
        # Try to get parties from the structured table
        complainant = response.xpath(
            '//td[contains(text(), "Complainant")]/following-sibling::td/text()'
        ).get()
        respondent = response.xpath(
            '//td[contains(text(), "Respondent")]/following-sibling::td/text()'
        ).get()
        if not complainant:
            # Alternative: parties table with headers
            complainant = response.xpath(
                '//th[contains(text(), "Complainant")]/../following-sibling::tr/td[1]/text()'
            ).get()
            respondent = response.xpath(
                '//th[contains(text(), "Respondent")]/../following-sibling::tr/td[2]/text()'
            ).get()

        if complainant and respondent:
            return f"{complainant.strip()} v {respondent.strip()}"

        # Fallback: first paragraph content
        first_p = response.css("p::text").get()
        return (first_p or "").strip()[:500]

    def _extract_date(self, response: HtmlResponse) -> str:
        """Extract decision date from detail page."""
        # Look for "Dated: DD/MM/YYYY" or "Date of Decision: ..."
        dated_match = response.xpath('//text()[contains(., "Dated:")]').re_first(
            r"Dated:\s*(\d{2}/\d{2}/\d{4})"
        )
        if dated_match:
            return dated_match

        # Look for "Date of Adjudication Hearing:"
        hearing_date = response.xpath(
            '//strong[contains(text(), "Date of Adjudication Hearing")]/following-sibling::text()'
        ).get()
        if hearing_date and hearing_date.strip():
            return hearing_date.strip()

        # Fallback: any date in DD/MM/YYYY format
        text = response.text[:10000]
        match = re.search(r"(\d{2}/\d{2}/\d{4})", text)
        if match:
            return match.group(1)

        return None

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime object."""
        if not date_str:
            return None
        fmts = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %B %Y", "%B %d, %Y"]
        for fmt in fmts:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        self.logger.warning(f"Could not parse date: {date_str}")
        return None

    # Links to ignore when looking for downloadable documents
    IGNORED_DOC_PATTERNS = [
        "cookie",
        "privacy",
        "terms",
        "footer",
        "policy",
        "accessibility",
        "disclaimer",
        "sitemap",
        "guide",
        "publications_forms",
        "information_guide",
    ]

    def _get_document_url(self, response: HtmlResponse) -> tuple:
        """Check for PDF/DOC download links on the detail page.

        Only considers links within the main content area (#main).
        Ignores common site-wide links like cookie_policy.pdf.
        """
        # Restrict to main content area to avoid footer/sidebar links
        main_content = response.css("#main")
        if not main_content:
            main_content = response

        doc_selectors = [
            'a[href$=".pdf"]',
            'a[href$=".PDF"]',
            'a[href$=".doc"]',
            'a[href$=".DOC"]',
            'a[href$=".docx"]',
            'a[href$=".DOCX"]',
        ]
        for sel in doc_selectors:
            links = main_content.css(f"{sel}::attr(href)").getall()
            for link in links:
                # Skip site-wide/footer links
                link_lower = link.lower()
                if any(pat in link_lower for pat in self.IGNORED_DOC_PATTERNS):
                    continue
                full_url = urljoin(self.base_url, link)
                ext = link.rsplit(".", 1)[-1].lower()
                doc_type = ext if ext in ("pdf", "doc", "docx") else "pdf"
                return full_url, doc_type

        # No downloadable document found — will capture HTML
        return None, "html"

    def handle_error(self, failure):
        """Handle request failures with structured logging."""
        url = failure.request.url
        self._log_structured(
            "request_failed",
            {
                "url": url,
                "error": str(failure.value),
                "error_type": failure.type.__name__ if failure.type else "Unknown",
                "body": self.body_name,
                "partition_date": self.partition_date,
            },
        )

    def handle_download_error(self, failure):
        """Handle document download failures — fall back to HTML content."""
        item = failure.request.meta.get("item")
        identifier = "unknown"
        if item:
            identifier = item.get("identifier", "unknown")

        error_info = {
            "url": failure.request.url,
            "identifier": identifier,
            "error": str(failure.value),
            "error_type": failure.type.__name__ if failure.type else "Unknown",
        }
        self._stats["downloads_failed"].append(error_info)
        self._log_structured("download_failed", error_info)

        # Fallback: if the PDF/DOC download failed, yield the item
        # with the detail page URL as the document source
        if item and isinstance(item, DecisionItem):
            source_url = item.get("source_url")
            if source_url:
                self._log_structured(
                    "download_fallback_html",
                    {
                        "identifier": identifier,
                        "source_url": source_url,
                    },
                )
                # Re-request the detail page to capture its HTML
                yield scrapy.Request(
                    url=source_url,
                    callback=self._save_html_fallback,
                    meta={
                        "item": item,
                        "impersonate": random.choice(IMPERSONATE_BROWSERS),
                    },
                    errback=self.handle_error,
                    dont_filter=True,
                )

    def _save_html_fallback(self, response: HtmlResponse):
        """Save the HTML page when PDF/DOC download failed."""
        item = response.meta["item"]
        item["document_type"] = "html"
        item["document_content"] = response.body
        item["link_to_doc"] = response.url
        self._stats["records_scraped"] += 1
        yield item

    def closed(self, reason):
        """Spider closed — emit final structured summary."""
        self._log_structured(
            "run_summary",
            {
                "body": self.body_name,
                "body_id": self.body_id,
                "partition_date": self.partition_date,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "records_found": self._stats["records_found"],
                "records_scraped": self._stats["records_scraped"],
                "records_skipped_duplicate": self._stats["records_skipped_duplicate"],
                "pages_crawled": self._stats["pages_crawled"],
                "downloads_failed_count": len(self._stats["downloads_failed"]),
                "downloads_failed": self._stats["downloads_failed"],
                "close_reason": reason,
            },
        )

    def _log_structured(self, event: str, data: dict):
        """Emit a structured JSON log entry."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "spider": self.name,
            "event": event,
            **data,
        }
        logger.info(json.dumps(log_entry))
