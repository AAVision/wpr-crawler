from dagster import job
from dagster_project.ops.scrape_op import (
    unified_pipeline_op,
    generate_scrape_tasks,
    scrape_body_partition,
    bulk_transform_op,
)


def create_full_pipeline_job():
    @job(name="full_pipeline")
    def full_pipeline():
        """The unified job that performs everything in one high-performance op."""
        unified_pipeline_op()

    return full_pipeline


def create_scrape_job():
    @job(name="partitioned_scrape_job")
    def partitioned_scrape():
        """The mapped job that waits for all scrapes to finish BEFORE transformation."""
        tasks = generate_scrape_tasks()
        # Collect all parallel scrape results into a list
        results = tasks.map(scrape_body_partition).collect()
        # Final synchronization point: Run transformation ONCE for everything
        bulk_transform_op(results)

    return partitioned_scrape
