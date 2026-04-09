from dagster import (
    op,
    Field,
    String,
    Int,
    DynamicOut,
    DynamicOutput,
    Any,
)
from dagster_project.workflow_runner import run_workflow
from transformer.transform import TransformationPipeline
from utils.metadata import BODIES

@op(
    config_schema={
        "start_date": Field(String, default_value="2020-01-01"),
        "end_date": Field(String, default_value="2020-12-31"),
        "partition_months": Field(Int, default_value=1),
        "body_id": Field(String, is_required=False),
        "bodies": Field([Any], is_required=False),
        "max_workers": Field(Int, is_required=False),
    }
)
def unified_pipeline_op(context):
    """The consolidated op that performs everything in one high-performance run."""
    config = context.op_config

    if config.get("bodies"):
        body_id = ",".join([str(b.get("id")) for b in config["bodies"] if b.get("id")])
    elif config.get("body_id"):
        body_id = config["body_id"]
    else:
        body_id = "all"

    summary = run_workflow(
        start_date=config["start_date"],
        end_date=config["end_date"],
        body_id=body_id,
        partition_months=config["partition_months"],
    )

    if summary["status"] == "Failed":
        raise Exception(f"Pipeline FAILED: {summary}")

    return summary


@op(
    config_schema={
        "start_date": Field(String, default_value="2020-01-01"),
        "end_date": Field(String, default_value="2020-12-31"),
        "bodies": Field([Any], default_value=BODIES),
    },
    out=DynamicOut(),
)
def generate_scrape_tasks(context):
    """Task generation for body-level parallelism."""
    bodies = context.op_config.get("bodies", BODIES)

    for body in bodies:
        task_data = {
            **body,
            "start_date": context.op_config["start_date"],
            "end_date": context.op_config["end_date"],
        }
        yield DynamicOutput(value=task_data, mapping_key=f"body_{body['id']}")


@op
def scrape_body_partition(context, task_data: dict):
    """Parallelized scrape worker."""
    summary = run_workflow(
        start_date=task_data["start_date"],
        end_date=task_data["end_date"],
        body_id=task_data["id"],
        skip_transform=True,
    )

    if summary["status"] == "Failed":
        raise Exception(f"Body scrape for {task_data.get('name')} FAILED")

    return summary


@op(
    config_schema={
        "start_date": Field(String, default_value="2020-01-01"),
        "end_date": Field(String, default_value="2020-12-31"),
        "max_workers": Field(Int, default_value=10),
    }
)
def bulk_transform_op(context, scrape_results: list):
    """Bulk transformation after parallel scrapes finish."""
    config = context.op_config

    context.log.info(f"Scrapes complete. Running bulk transform ({config['start_date']} to {config['end_date']})")

    pipeline = TransformationPipeline()
    try:
        return pipeline.run(
            start_date=config["start_date"],
            end_date=config["end_date"],
            max_workers=config["max_workers"],
        )
    finally:
        pipeline.close()
