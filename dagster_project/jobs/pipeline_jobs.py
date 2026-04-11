from dagster import job
from dagster_project.ops.scrape_op import (
    unified_pipeline_op,
)

def create_full_pipeline_job():
    @job(name="full_pipeline")
    def full_pipeline():
        """The unified job that performs everything in one high-performance op."""
        unified_pipeline_op()

    return full_pipeline
