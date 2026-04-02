import subprocess
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dagster import op, Field, String, Int, DynamicOutput, DynamicOut
from utils.logging_utils import setup_logging

# Centralized Logging
logger = setup_logging(__name__)

# Correct body IDs matching the actual website
BODIES = [
    {"id": "1", "name": "Employment Appeals Tribunal"},
    {"id": "2", "name": "Equality Tribunal"},
    {"id": "3", "name": "Labour Court"},
    {"id": "15376", "name": "Workplace Relations Commission"},
]


def get_partitions(start_date: datetime, end_date: datetime, months: int = 1):
    """Generate monthly partitions between start and end dates."""
    partitions = []
    current = start_date.replace(day=1)
    while current <= end_date:
        partition_start = current
        partition_end = min(
            current + relativedelta(months=months) - timedelta(days=1), end_date
        )
        partitions.append(
            (
                partition_start.strftime("%Y-%m-%d"),
                partition_end.strftime("%Y-%m-%d"),
                current.strftime("%Y-%m"),
            )
        )
        current += relativedelta(months=months)
    return partitions


@op(
    config_schema={
        "start_date": Field(String),
        "end_date": Field(String),
        "partition_months": Field(Int, default_value=1),
        "bodies": Field(list, is_required=False, default_value=BODIES),
    },
    out=DynamicOut(),
)
def generate_scrape_tasks(context):
    """Generate dynamic tasks for each body and partition combination."""
    start_date = datetime.strptime(context.op_config["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(context.op_config["end_date"], "%Y-%m-%d")
    partition_months = context.op_config.get("partition_months", 1)
    bodies = context.op_config.get("bodies", BODIES)

    partitions = get_partitions(start_date, end_date, partition_months)

    for i, (ps, pe, plabel) in enumerate(partitions):
        for body in bodies:
            # Create a unique key for Dagster tracking
            # Format: YYYY_MM_BODYID (must be alphanumeric and underscores)
            tag = f"{plabel.replace('-', '_')}_{body['id']}"

            task_data = {
                "start_date": ps,
                "end_date": pe,
                "body_id": body["id"],
                "body_name": body["name"],
                "partition_date": plabel,
            }

            yield DynamicOutput(value=task_data, mapping_key=tag)


@op
def scrape_single_body(context, task_data: dict):
    """Execute a single spider run for one body and one partition in parallel."""
    ps = task_data["start_date"]
    pe = task_data["end_date"]
    bid = task_data["body_id"]
    bname = task_data["body_name"]
    plabel = task_data["partition_date"]

    context.log.info(f"Starting parallel scrape for {bname} in partition {plabel}")

    try:
        result = run_spider(ps, pe, bid, bname, plabel)
        # Add basic identification
        result["body_name"] = bname
        result["partition"] = plabel
        return result
    except Exception as e:
        context.log.error(f"Failed to scrape {bname} for {plabel}: {str(e)}")
        raise e


@op
def consolidate_results(context, results: list):
    """Aggregate all parallel results into a single summary."""
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_tasks": len(results),
        "total_records_found": sum(r.get("records_found", 0) for r in results),
        "total_records_scraped": sum(r.get("records_scraped", 0) for r in results),
        "failed_documents": [],
    }
    for r in results:
        if r.get("failed_downloads"):
            summary["failed_documents"].extend(r["failed_downloads"])

    context.log.info(f"Parallel Scrape Completed: {summary}")
    return summary


def run_spider(start_date, end_date, body_id, body_name, partition_date):
    """Execute a single spider run for a specific body and date range."""
    cmd = [
        "scrapy",
        "crawl",
        "wr_spider",
        "-a",
        f"start_date={start_date}",
        "-a",
        f"end_date={end_date}",
        "-a",
        f"body_id={body_id}",
        "-a",
        f"body_name={body_name}",
        "-a",
        f"partition_date={partition_date}",
        "-s",
        "LOG_LEVEL=INFO",
    ]

    result = subprocess.run(
        cmd,
        cwd="/opt/dagster/scrapy_project",
        capture_output=True,
        text=True,
        timeout=3600,
    )

    if result.returncode != 0:
        raise Exception(
            f"Scrapy failed for {body_name} [{partition_date}]: {result.stderr[-500:]}"
        )

    # Try to parse stats from spider output (look for the run_summary JSON log)
    stats = {
        "records_found": 0,
        "records_scraped": 0,
        "failed_downloads": [],
        "start_date": start_date,
        "end_date": end_date,
    }
    for line in result.stdout.split("\n"):
        if '"event": "run_summary"' in line or '"event":"run_summary"' in line:
            try:
                # Extract JSON from the log line
                json_start = line.index("{")
                data = json.loads(line[json_start:])
                stats["records_found"] = data.get("records_found", 0)
                stats["records_scraped"] = data.get("records_scraped", 0)
                stats["failed_downloads"] = data.get("downloads_failed", [])
            except (ValueError, json.JSONDecodeError):
                pass

    return stats
