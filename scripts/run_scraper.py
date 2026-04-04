import argparse
import sys
from dagster_project.workflow_runner import run_workflow, BODIES
from utils.logging_utils import setup_logging

logger = setup_logging(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run WR spider CLI (Unified)")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--body-id", help="Scrape only a specific body ID")
    parser.add_argument(
        "--partition-months", type=int, default=1, help="Months per partition"
    )
    parser.add_argument("--max-workers", type=int, help="Override parallel workers")
    args = parser.parse_args()

    # Pre-validation for body_id
    if args.body_id:
        if not any(b["id"] == args.body_id for b in BODIES):
            logger.error(
                f"Unknown body_id: {args.body_id}. Valid: {[b['id'] for b in BODIES]}"
            )
            sys.exit(1)

    try:
        summary = run_workflow(
            start_date=args.start_date,
            end_date=args.end_date,
            body_id=args.body_id,
            partition_months=args.partition_months,
            max_workers=args.max_workers,
        )
        sys.exit(0 if summary["failed"] == 0 else 1)
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
