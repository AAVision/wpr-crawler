from dagster import op, Field, String
from transformer.transform import TransformationPipeline
from utils.logging_utils import setup_logging

# Centralized Logging
logger = setup_logging(__name__)

@op(config_schema={
    "start_date": Field(String, is_required=False),
    "end_date": Field(String, is_required=False),
})
def transform_documents(context, scrape_summary: dict = None):
    start_date = context.op_config.get("start_date")
    end_date = context.op_config.get("end_date")
    pipeline = TransformationPipeline()
    try:
        stats = pipeline.run(start_date=start_date, end_date=end_date)
        context.log.info(f"Transformation completed: {stats}")
        return stats
    finally:
        pipeline.close()