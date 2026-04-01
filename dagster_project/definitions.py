from dagster import Definitions, ScheduleDefinition
from dagster_project.jobs import pipeline_jobs

scrape_job = pipeline_jobs.create_scrape_job()
transform_job = pipeline_jobs.create_transform_job()
full_pipeline_job = pipeline_jobs.create_full_pipeline_job()

daily_schedule = ScheduleDefinition(
    name="daily_scrape_schedule",
    cron_schedule="0 9 * * *",
    job=scrape_job,
)

defs = Definitions(
    jobs=[scrape_job, transform_job, full_pipeline_job],
    schedules=[daily_schedule],
)