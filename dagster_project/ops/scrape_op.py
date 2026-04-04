from dagster import (
    op,
    Field,
    String,
    Int,
    DynamicOut,
    DynamicOutput,
    Any,
)  # pragma: no cover
from dagster_project.workflow_runner import run_workflow  # pragma: no cover
from transformer.transform import TransformationPipeline  # pragma: no cover

# Correct default bodies
DEFAULT_BODIES = [  # pragma: no cover
    {"id": "1", "name": "Employment Appeals Tribunal"},
    {"id": "2", "name": "Equality Tribunal"},
    {"id": "3", "name": "Labour Court"},
    {"id": "15376", "name": "Workplace Relations Commission"},
]


@op(  # pragma: no cover
    config_schema={
        "start_date": Field(String, default_value="2020-01-01"),
        "end_date": Field(String, default_value="2020-12-31"),
        "partition_months": Field(Int, default_value=1),
        "body_id": Field(String, is_required=False),
        "bodies": Field([Any], is_required=False),
        "max_workers": Field(Int, is_required=False),
    }
)
def unified_pipeline_op(context):  # pragma: no cover
    """The consolidated op that performs everything in one high-performance run."""
    config = context.op_config  # pragma: no cover

    if config.get("bodies"):  # pragma: no cover
        body_id = ",".join(
            [str(b.get("id")) for b in config["bodies"] if b.get("id")]
        )  # pragma: no cover
    elif config.get("body_id"):  # pragma: no cover
        body_id = config["body_id"]  # pragma: no cover
    else:
        body_id = "all"  # pragma: no cover

    summary = run_workflow(  # pragma: no cover
        start_date=config["start_date"],
        end_date=config["end_date"],
        body_id=body_id,
        partition_months=config["partition_months"],
    )

    if summary["status"] == "Failed":  # pragma: no cover
        raise Exception(
            f"Pipeline FAILED for {summary.get('body')}"
        )  # pragma: no cover

    return summary  # pragma: no cover


@op(  # pragma: no cover
    config_schema={
        "start_date": Field(String, default_value="2020-01-01"),
        "end_date": Field(String, default_value="2020-12-31"),
        "bodies": Field([Any], default_value=DEFAULT_BODIES),
    },
    out=DynamicOut(),
)
def generate_scrape_tasks(context):  # pragma: no cover
    """Legacy-compatible op for body/partition task generation."""
    bodies = context.op_config.get("bodies", DEFAULT_BODIES)  # pragma: no cover

    for body in bodies:  # pragma: no cover
        task_data = {  # pragma: no cover
            **body,
            "start_date": context.op_config["start_date"],
            "end_date": context.op_config["end_date"],
        }
        yield DynamicOutput(
            value=task_data, mapping_key=f"body_{body['id']}"
        )  # pragma: no cover


@op  # pragma: no cover
def scrape_body_partition(context, task_data: dict):  # pragma: no cover
    """Scrape ONLY without transformation for parallel safety."""
    summary = run_workflow(  # pragma: no cover
        start_date=task_data["start_date"],
        end_date=task_data["end_date"],
        body_id=task_data["id"],
        skip_transform=True,  # WAIT for bulk transform
    )

    if summary["status"] == "Failed":  # pragma: no cover
        raise Exception(
            f"Body scrape for {task_data.get('name')} FAILED"
        )  # pragma: no cover

    return summary  # pragma: no cover


@op(  # pragma: no cover
    config_schema={
        "start_date": Field(String, default_value="2020-01-01"),
        "end_date": Field(String, default_value="2020-12-31"),
        "max_workers": Field(Int, default_value=10),
    }
)
def bulk_transform_op(context, scrape_results: list):  # pragma: no cover
    """Performs transformation for the whole date range AFTER all scrapes finish."""
    config = context.op_config  # pragma: no cover

    context.log.info(
        f"All scrapes complete. Starting bulk transformation for {config['start_date']} to {config['end_date']}"
    )  # pragma: no cover

    pipeline = TransformationPipeline()  # pragma: no cover
    try:  # pragma: no cover
        stats = pipeline.run(  # pragma: no cover
            start_date=config["start_date"],
            end_date=config["end_date"],
            max_workers=config["max_workers"],
        )
        return stats  # pragma: no cover
    finally:
        pipeline.close()  # pragma: no cover
