import os
import logging
import json
from datetime import datetime
from logging.handlers import RotatingFileHandler


def setup_logging(name=None, level=None):
    """
    Centralized logging configuration.
    Sets up a consistent format and level across all modules.
    """
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO").upper()

    # Use a standard professional format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logging.basicConfig(level=level, format=log_format, force=True)

    root_logger = logging.getLogger()

    if not any(isinstance(h, RotatingFileHandler) for h in root_logger.handlers):
        log_dir = os.getenv("LOG_DIR", os.path.join(os.getcwd(), "logs"))
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)  # pragma: no cover

        log_file = os.path.join(log_dir, "pipeline.log")

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
        )
        file_handler.setFormatter(logging.Formatter(log_format))
        file_handler.setLevel(level)
        root_logger.addHandler(file_handler)

    return logging.getLogger(name or __name__)


def log_structured(logger, event, data=None, level=logging.INFO):
    """
    Emits a structured JSON log entry for machine parsing.
    Useful for Dagster and monitoring tools.
    """
    entry = {
        "timestamp": datetime.now().isoformat(),
        "event": event,
        **(data or {}),
    }  # pragma: no cover
    logger.log(level, json.dumps(entry))  # pragma: no cover
