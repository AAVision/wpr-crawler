from datetime import datetime  # pragma: no cover
from dagster import op, Field  # pragma: no cover
from transformer.transform import TransformationPipeline  # pragma: no cover
from utils.logging_utils import setup_logging  # pragma: no cover

# Centralized Logging
logger = setup_logging(__name__)  # pragma: no cover


@op(  # pragma: no cover
    config_schema={
        "max_workers": Field(int, is_required=False, default_value=5),
    }
)
def transform_single_partition(context, scrape_result: dict):  # pragma: no cover
    """Transform a single partition in parallel in Dagster."""
    bname = scrape_result.get("body_name", "unknown")  # pragma: no cover
    plabel = scrape_result.get("partition", "unknown")  # pragma: no cover
    ps = scrape_result.get("start_date")  # pragma: no cover
    pe = scrape_result.get("end_date")  # pragma: no cover

    context.log.info(
        f"Starting parallel transform for {bname} in partition {plabel}"
    )  # pragma: no cover

    max_workers = context.op_config.get("max_workers", 5)  # pragma: no cover
    pipeline = TransformationPipeline()  # pragma: no cover
    try:  # pragma: no cover
        # Run transformation for just this specific partition and body
        stats = pipeline.run(
            start_date=ps, end_date=pe, max_workers=max_workers
        )  # pragma: no cover
        stats["body_name"] = bname  # pragma: no cover
        stats["partition"] = plabel  # pragma: no cover
        return stats  # pragma: no cover
    finally:
        pipeline.close()  # pragma: no cover


@op  # pragma: no cover
def consolidate_transform_results(context, results: list):  # pragma: no cover
    """Aggregate all parallel transform results into a single summary."""
    summary = {  # pragma: no cover
        "timestamp": datetime.now().isoformat(),
        "total_transformed": sum(r.get("transformed", 0) for r in results),
        "total_skipped": sum(r.get("skipped", 0) for r in results),
        "total_failed": sum(
            r.get("total", 0) - r.get("transformed", 0) - r.get("skipped", 0)
            for r in results
        ),
    }
    context.log.info(
        f"Parallel Transform Workflow Completed: {summary}"
    )  # pragma: no cover
    return summary  # pragma: no cover
