import subprocess
import os
import sys
from transformer.transform import TransformationPipeline
from utils.logging_utils import setup_logging
from utils.metadata import BODIES

logger = setup_logging(__name__)

def run_workflow(
    start_date,
    end_date,
    body_id=None,
    partition_months=1,
    max_workers=4,
    skip_transform=False,
):
    """Unified Runner: Primary entry point for Scrapy-driven discovery and extraction."""
    target_body_id = body_id if body_id else ",".join([b["id"] for b in BODIES])
    
    logger.info(f"Starting Scrapy-driven pipeline for {start_date} to {end_date}")
    
    cmd = [
        "scrapy",
        "crawl",
        "wr_spider",
        "-a", f"start_date={start_date}",
        "-a", f"end_date={end_date}",
        "-a", f"body_id={target_body_id}",
        "-a", f"partition_months={partition_months or 1}",
    ]
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scrapy_dir = os.path.join(base_dir, "scrapy_project")
    
    env = os.environ.copy()
    env["PYTHONPATH"] = base_dir
    
    process = subprocess.Popen(
        cmd,
        env=env,
        cwd=scrapy_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    for line in iter(process.stdout.readline, ""):
        sys.stdout.write(line)
        sys.stdout.flush()

    process.stdout.close()
    ret = process.wait()

    summary = {
        "status": "Success" if ret == 0 else "Failed",
        "failed": 0 if ret == 0 else 1,
        "start_date": start_date,
        "end_date": end_date,
        "body_id": target_body_id,
    }

    if ret == 0 and not skip_transform:
        logger.info("Starting Transformation Phase...")
        try:
            pipeline = TransformationPipeline()
            pipeline.run(start_date=start_date, end_date=end_date)
            pipeline.close()
        except Exception as e:
            logger.error(f"Transformation failed: {e}")

    return summary
