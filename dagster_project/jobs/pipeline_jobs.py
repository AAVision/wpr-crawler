import psutil
from dagster import job, multiprocess_executor
from dagster_project.ops.scrape_op import generate_scrape_tasks, scrape_single_body, consolidate_results
from dagster_project.ops.transform_op import transform_single_partition, consolidate_transform_results

def get_safe_concurrency_limit():
    """Calculate a safe parallel task limit based on available physical RAM."""
    try:
        mem = psutil.virtual_memory()
        # Estimate 500MB per scraper task (Python + Playwright/Chromium)
        # Use 80% of available RAM as a safety buffer
        safe_ram = mem.available * 0.8
        limit = int(safe_ram / (500 * 1024 * 1024))
        # Keep it within a reasonable range (min 2, max 16 for local laptop)
        return max(2, min(limit, 16))
    except Exception:
        return 4 # Defensive fallback

# Dynamic executor based on local hardware capacity
local_executor = multiprocess_executor.configured({"max_concurrent": get_safe_concurrency_limit()})

def create_scrape_job():
    @job(name="scrape_workplace_relations_job", executor_def=local_executor)
    def scrape_job():
        tasks = generate_scrape_tasks()
        results = tasks.map(scrape_single_body)
        consolidate_results(results.collect())
    return scrape_job

def create_transform_job():
    @job(name="transform_documents_job", executor_def=local_executor)
    def transform_job():
        # Standalone transform is rarely used but we keep the protection
        pass
    return transform_job

def create_full_pipeline_job():
    @job(name="full_pipeline", executor_def=local_executor)
    def full_pipeline():
        # Parallel Scraping
        tasks = generate_scrape_tasks()
        scrape_results = tasks.map(scrape_single_body)
        
        # Parallel Transformation
        transform_results = scrape_results.map(transform_single_partition)
        
        # Aggregation
        consolidate_transform_results(transform_results.collect())
    return full_pipeline
