from dagster import job
from dagster_project.ops.scrape_op import scrape_documents
from dagster_project.ops.transform_op import transform_documents

def create_scrape_job():
    @job(name="scrape_workplace_relations")
    def scrape_job():
        scrape_documents()
    return scrape_job

def create_transform_job():
    @job(name="transform_documents")
    def transform_job():
        transform_documents()
    return transform_job

def create_full_pipeline_job():
    @job(name="full_pipeline")
    def full_pipeline():
        scrape_result = scrape_documents()
        transform_documents(scrape_result)
    return full_pipeline