import subprocess
import os
import sys
import json
import asyncio
from typing import List
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from transformer.transform import TransformationPipeline
from utils.logging_utils import setup_logging, log_structured

logger = setup_logging(__name__)

# Correct body IDs
BODIES = [
    {"id": "1", "name": "Employment Appeals Tribunal"},
    {"id": "2", "name": "Equality Tribunal"},
    {"id": "3", "name": "Labour Court"},
    {"id": "15376", "name": "Workplace Relations Commission"},
]

class WRDiscoveryEngine:
    """Async Discovery Engine: Concurrent pages on one browser."""
    
    def __init__(self, browser):
        self.browser = browser
        self.base_url = "https://www.workplacerelations.ie"
        self.search_url = f"{self.base_url}/en/search/"

    async def discover_partition(self, body_id: str, start_date: str, end_date: str) -> List[str]:
        """Discover URLs for a single partition asynchronously."""
        context = await self.browser.new_context()
        page = await context.new_page()
        urls = []
        
        try:
            # Format dates
            sd_dt = datetime.strptime(start_date, "%Y-%m-%d")
            ed_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            import urllib.parse
            body_list = body_id.split(",") if isinstance(body_id, str) else [str(body_id)]
            params = [
                ("decisions", "1"),
                ("from", sd_dt.strftime("%d/%m/%Y")),
                ("to", ed_dt.strftime("%d/%m/%Y"))
            ]
            for b in body_list:
                params.append(("body", b.strip()))
                
            url = f"{self.search_url}?{urllib.parse.urlencode(params)}"
            
            logger.info(f"Searching: {start_date} to {end_date}")
            await page.goto(url, wait_until="networkidle", timeout=60000)
            
            while True:
                await page.wait_for_selector("li.each-item, .results-count", timeout=50000)
                
                # Extract
                links = await page.eval_on_selector_all(
                    "li.each-item a.btn-primary",
                    "elements => elements.map(el => el.href)"
                )
                urls.extend(links)
                
                next_btn = await page.query_selector("a.next")
                if next_btn and await next_btn.is_visible():
                    await next_btn.click()
                    await page.wait_for_load_state("networkidle")
                else:
                    break
            return urls
        except Exception as e:
            logger.error(f"Discovery error {start_date}: {e}")
            return []
        finally:
            await page.close()
            await context.close()

async def async_discovery(start_date, end_date, body_id, partition_months, max_workers):
    """Run Discovery Phase using asyncio for stability."""
    all_urls = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        engine = WRDiscoveryEngine(browser)
        
        # Partitioning
        partitions = []
        curr = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
        ed = datetime.strptime(end_date, "%Y-%m-%d")
        while curr <= ed:
            p_start = curr.strftime("%Y-%m-%d")
            p_end = min(curr + relativedelta(months=partition_months) - timedelta(days=1), ed).strftime("%Y-%m-%d")
            partitions.append((p_start, p_end))
            curr += relativedelta(months=partition_months)

        # Semaphore limiting
        semaphore = asyncio.Semaphore(max_workers)
        async def limited_discover(s, e):
            async with semaphore:
                return await engine.discover_partition(body_id, s, e)

        tasks = [limited_discover(s, e) for s, e in partitions]
        results = await asyncio.gather(*tasks)
        for r in results:
            all_urls.extend(r)
        await browser.close()
    return all_urls

def run_workflow(start_date, end_date, body_id=None, partition_months=1, max_workers=4, skip_transform=False):
    """Unified Runner with optional transformation step."""
    target_body_id = body_id if body_id else "1,2,3,15376"
    p_months = int(partition_months or 1)
    m_workers = int(max_workers or 4)
    
    # Phase 1: Async Discovery
    logger.info("Phase 1: Async Discovery starting...")
    try:
        all_urls = asyncio.run(async_discovery(start_date, end_date, target_body_id, p_months, m_workers))
    except Exception as e:
        logger.error(f"Discovery phase failed: {e}")
        return {"status": "Failed", "message": str(e), "failed": 1}

    if not all_urls:
        logger.info(f"Phase 1 complete: Found 0 URLs for {start_date} to {end_date}. Returning.")
        return {"status": "Complete", "records": 0, "message": "No URLs found", "failed": 0}

    logger.info(f"Phase 1 complete: Found {len(all_urls)} URLs. Starting Phase 2: High-speed extraction...")
    
    url_file = f"/tmp/scraped_urls_{os.getpid()}.json"
    with open(url_file, "w") as f:
        json.dump(all_urls, f)

    cmd = [
        "scrapy", "crawl", "wr_spider", 
        "-a", f"url_list_file={url_file}",
        "-a", f"start_date={start_date}",
        "-a", f"end_date={end_date}",
        "-a", f"body_id={target_body_id}",
        "-a", f"partition_date={start_date[:7] if start_date else ''}"
    ]
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scrapy_dir = os.path.join(base_dir, "scrapy_project")
    
    env = os.environ.copy()
    env["PYTHONPATH"] = base_dir
    
    process = subprocess.Popen(
        cmd, env=env, cwd=scrapy_dir,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1
    )

    for line in iter(process.stdout.readline, ""):
        sys.stdout.write(line)
        sys.stdout.flush()

    process.stdout.close()
    ret = process.wait()

    summary = {
        "status": "Success" if ret == 0 else "Failed",
        "records": len(all_urls),
        "failed": 0 if ret == 0 else 1,
        "start_date": start_date,
        "end_date": end_date,
        "body_id": target_body_id
    }

    # Phase 3: Transformation (Skip if managed by external job)
    if ret == 0 and not skip_transform:
        logger.info("Phase 3: Transformation starting...")
        try:
            pipeline = TransformationPipeline()
            pipeline.run(start_date=start_date, end_date=end_date)
            pipeline.close()
        except Exception as e:
            logger.error(f"Transformation failed: {e}")

    # Cleanup temp file
    if os.path.exists(url_file):
        os.remove(url_file)

    return summary
