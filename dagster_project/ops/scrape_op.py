import subprocess
import json
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dagster import op, Field, String, Int, Dict as DagsterDict

logger = logging.getLogger(__name__)

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
            current + relativedelta(months=months) - timedelta(days=1),
            end_date
        )
        partitions.append((
            partition_start.strftime('%Y-%m-%d'),
            partition_end.strftime('%Y-%m-%d'),
            current.strftime('%Y-%m')
        ))
        current += relativedelta(months=months)
    return partitions


@op(config_schema={
    "start_date": Field(String),
    "end_date": Field(String),
    "partition_months": Field(Int, default_value=1),
    "bodies": Field(DagsterDict, is_required=False, default_value={"bodies": BODIES}),
})
def scrape_documents(context):
    """Orchestrate scraping across all bodies and time partitions."""
    start_date = datetime.strptime(context.op_config["start_date"], '%Y-%m-%d')
    end_date = datetime.strptime(context.op_config["end_date"], '%Y-%m-%d')
    partition_months = context.op_config.get("partition_months", 1)
    bodies = context.op_config.get("bodies", {"bodies": BODIES}).get("bodies", BODIES)

    partitions = get_partitions(start_date, end_date, partition_months)

    summary = {
        "timestamp": datetime.now().isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_partitions": len(partitions),
        "total_bodies": len(bodies),
        "partitions_processed": 0,
        "bodies_processed": 0,
        "total_records_found": 0,
        "total_records_scraped": 0,
        "failed_partitions": [],
        "failed_documents": [],
    }

    for ps, pe, plabel in partitions:
        context.log.info(json.dumps({
            "event": "partition_start",
            "partition": plabel,
            "start": ps,
            "end": pe,
        }))

        for body in bodies:
            context.log.info(json.dumps({
                "event": "body_scrape_start",
                "partition": plabel,
                "body_id": body['id'],
                "body_name": body['name'],
            }))

            try:
                result = run_spider(ps, pe, body['id'], body['name'], plabel)
                summary["total_records_found"] += result.get("records_found", 0)
                summary["total_records_scraped"] += result.get("records_scraped", 0)
                summary["bodies_processed"] += 1
                if result.get("failed_downloads"):
                    summary["failed_documents"].extend(result["failed_downloads"])
            except Exception as e:
                error_info = {
                    "partition": plabel,
                    "body_id": body['id'],
                    "body_name": body['name'],
                    "error": str(e),
                }
                summary["failed_partitions"].append(error_info)
                context.log.error(json.dumps({
                    "event": "body_scrape_failed",
                    **error_info,
                }))

        summary["partitions_processed"] += 1

    # Final run summary
    context.log.info(json.dumps({
        "event": "run_summary",
        **summary,
    }, indent=2))

    return summary


def run_spider(start_date, end_date, body_id, body_name, partition_date):
    """Execute a single spider run for a specific body and date range."""
    cmd = [
        "scrapy", "crawl", "wr_spider",
        "-a", f"start_date={start_date}",
        "-a", f"end_date={end_date}",
        "-a", f"body_id={body_id}",
        "-a", f"body_name={body_name}",
        "-a", f"partition_date={partition_date}",
        "-s", "LOG_LEVEL=INFO",
    ]

    result = subprocess.run(
        cmd,
        cwd="/opt/dagster/scrapy_project",
        capture_output=True,
        text=True,
        timeout=3600,
    )

    if result.returncode != 0:
        raise Exception(f"Scrapy failed for {body_name} [{partition_date}]: {result.stderr[-500:]}")

    # Try to parse stats from spider output (look for the run_summary JSON log)
    stats = {"records_found": 0, "records_scraped": 0, "failed_downloads": []}
    for line in result.stdout.split('\n'):
        if '"event": "run_summary"' in line or '"event":"run_summary"' in line:
            try:
                # Extract JSON from the log line
                json_start = line.index('{')
                data = json.loads(line[json_start:])
                stats["records_found"] = data.get("records_found", 0)
                stats["records_scraped"] = data.get("records_scraped", 0)
                stats["failed_downloads"] = data.get("downloads_failed", [])
            except (ValueError, json.JSONDecodeError):
                pass

    return stats