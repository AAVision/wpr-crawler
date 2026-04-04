from utils.logging_utils import setup_logging
import logging


def test_setup_logging():
    logger = setup_logging()
    assert logger is not None
    assert isinstance(logger, logging.Logger)
