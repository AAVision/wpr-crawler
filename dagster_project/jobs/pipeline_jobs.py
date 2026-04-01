from dagster import job
from dagster_project.ops.scrape_op import generate_scrape_tasks, scrape_single_body, consolidate_results
from dagster_project.ops.transform_op import transform_documents

def create_scrape_job():
    @job(name="scrape_workplace_relations_job")
    def scrape_job():
        tasks = generate_scrape_tasks()
        results = tasks.map(scrape_single_body)
        consolidate_results(results.collect())
    return scrape_job

def create_transform_job():
    @job(name="transform_documents_job")
    def transform_job():
        transform_documents()
    return transform_job

def create_full_pipeline_job():
    @job(name="full_pipeline")
    def full_pipeline():
        tasks = generate_scrape_tasks()
        results = tasks.map(scrape_single_body)
        summary = consolidate_results(results.collect())
        transform_documents(summary)
    return full_pipeline