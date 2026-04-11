from dagster import (
    op,
    Field,
    String,
    Int,
    Any,
)
from dagster_project.workflow_runner import run_workflow

@op(
    config_schema={
        "start_date": Field(String, default_value="2020-01-01"),
        "end_date": Field(String, default_value="2020-12-31"),
        "partition_months": Field(Int, default_value=1),
        "body_id": Field(String, is_required=False),
        "bodies": Field([Any], is_required=False),
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
