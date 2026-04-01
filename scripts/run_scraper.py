#!/usr/bin/env python3
"""
Run the WR spider across all bodies and monthly partitions.

Usage:
    python scripts/run_scraper.py --start-date 2024-01-01 --end-date 2025-01-01
    python scripts/run_scraper.py --start-date 2024-01-01 --end-date 2025-01-01 --body-id 15376
"""
import argparse
import subprocess
import sys
import os
import json
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from transformer.transform import TransformationPipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

# Correct body IDs from the actual website
BODIES = [
    {"id": "1", "name": "Employment Appeals Tribunal"},
    {"id": "2", "name": "Equality Tribunal"},
    {"id": "3", "name": "Labour Court"},
    {"id": "15376", "name": "Workplace Relations Commission"},
]


def get_partitions(start_date: datetime, end_date: datetime, months: int = 1):
    """Generate monthly partitions between two dates."""
    partitions = []
    current = start_date.replace(day=1)
    while current <= end_date:
        p_start = current
        p_end = min(current + relativedelta(months=months) - timedelta(days=1), end_date)
        partitions.append((
            p_start.strftime('%Y-%m-%d'),
            p_end.strftime('%Y-%m-%d'),
            current.strftime('%Y-%m'),
        ))
        current += relativedelta(months=months)
    return partitions


def run_spider(start_date, end_date, body_id, body_name, partition_date):
    """Execute the spider for one body and one partition."""
    cmd = [
        "scrapy", "crawl", "wr_spider",
        "-a", f"start_date={start_date}",
        "-a", f"end_date={end_date}",
        "-a", f"body_id={body_id}",
        "-a", f"body_name={body_name}",
        "-a", f"partition_date={partition_date}",
    ]
    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    project_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scrapy_project')

    logger.info(json.dumps({
        "event": "spider_start",
        "body": body_name,
        "body_id": body_id,
        "partition": partition_date,
        "start_date": start_date,
        "end_date": end_date,
    }))

    result = subprocess.run(cmd, env=env, cwd=project_dir, timeout=3600)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description='Run WR spider with date partitioning')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--body-id', help='Scrape only a specific body ID')
    parser.add_argument('--partition-months', type=int, default=1, help='Months per partition (default: 1)')
    args = parser.parse_args()

    start = datetime.strptime(args.start_date, '%Y-%m-%d')
    end = datetime.strptime(args.end_date, '%Y-%m-%d')
    partitions = get_partitions(start, end, args.partition_months)

    # Filter bodies if specific body_id provided
    bodies = BODIES
    if args.body_id:
        bodies = [b for b in BODIES if b['id'] == args.body_id]
        if not bodies:
            logger.error(f"Unknown body_id: {args.body_id}. Valid: {[b['id'] for b in BODIES]}")
            sys.exit(1)

    summary = {
        "total_partitions": len(partitions),
        "total_bodies": len(bodies),
        "successful_runs": 0,
        "failed_runs": 0,
        "failures": [],
    }

    logger.info(json.dumps({
        "event": "scraper_start",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "partitions": len(partitions),
        "bodies": [b['name'] for b in bodies],
    }))

    for ps, pe, plabel in partitions:
        for body in bodies:
            try:
                returncode = run_spider(ps, pe, body['id'], body['name'], plabel)
                if returncode == 0:
                    summary['successful_runs'] += 1
                else:
                    summary['failed_runs'] += 1
                    summary['failures'].append({
                        'partition': plabel, 'body': body['name'],
                        'error': f'Exit code {returncode}',
                    })
            except Exception as e:
                summary['failed_runs'] += 1
                summary['failures'].append({
                    'partition': plabel, 'body': body['name'], 'error': str(e),
                })

    logger.info(json.dumps({"event": "scraper_complete", **summary}, indent=2))

    # Run the transformation pipeline after successful scrape entries
    if summary['successful_runs'] > 0:
        logger.info("Starting transformation pipeline...")
        try:
            pipeline = TransformationPipeline()
            # Pass the overall start/end date for processing
            pipeline.run(start_date=args.start_date, end_date=args.end_date)
            pipeline.close()
            logger.info("Transformation pipeline completed.")
        except Exception as e:
            logger.error(f"Transformation pipeline failed: {e}")

    sys.exit(0 if summary['failed_runs'] == 0 else 1)


if __name__ == '__main__':
    main()