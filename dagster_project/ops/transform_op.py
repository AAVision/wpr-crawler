from datetime import datetime
from dagster import op, Field
from transformer.transform import TransformationPipeline
from utils.logging_utils import setup_logging

# Centralized Logging
logger = setup_logging(__name__)

@op(config_schema={
    "max_workers": Field(int, is_required=False, default_value=5),
})
def transform_single_partition(context, scrape_result: dict):
    """Transform a single partition in parallel in Dagster."""
    bname = scrape_result.get("body_name", "unknown")
    plabel = scrape_result.get("partition", "unknown")
    ps = scrape_result.get("start_date")
    pe = scrape_result.get("end_date")
    
    context.log.info(f"Starting parallel transform for {bname} in partition {plabel}")
    
    max_workers = context.op_config.get("max_workers", 5)
    pipeline = TransformationPipeline()
    try:
        # Run transformation for just this specific partition and body
        stats = pipeline.run(start_date=ps, end_date=pe, max_workers=max_workers)
        stats["body_name"] = bname
        stats["partition"] = plabel
        return stats
    finally:
        pipeline.close()

@op
def consolidate_transform_results(context, results: list):
    """Aggregate all parallel transform results into a single summary."""
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_transformed": sum(r.get("transformed", 0) for r in results),
        "total_skipped": sum(r.get("skipped", 0) for r in results),
        "total_failed": sum(r.get("total", 0) - r.get("transformed", 0) - r.get("skipped", 0) for r in results),
    }
    context.log.info(f"Parallel Transform Workflow Completed: {summary}")
    return summary
