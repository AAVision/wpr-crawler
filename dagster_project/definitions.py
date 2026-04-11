from dagster import Definitions, ScheduleDefinition
from dagster_project.jobs import pipeline_jobs

# Primary Job: Scrape + Transform (Matches run_scraper.py)
full_pipeline_job = pipeline_jobs.create_full_pipeline_job()

daily_schedule = ScheduleDefinition(
    name="daily_all_bodies_schedule",
    cron_schedule="0 9 * * *",
    job=full_pipeline_job,
)

defs = Definitions(
    jobs=[full_pipeline_job],
    schedules=[daily_schedule],
)
