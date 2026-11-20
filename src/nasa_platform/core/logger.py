import logging
import sys


def setup_logging(level=logging.INFO):
    """Configure root logger to ensure all module logs print to stdout."""
    logging.basicConfig(
        stream=sys.stdout,
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )