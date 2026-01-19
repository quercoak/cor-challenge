"""Create initial database from models."""

import logging

from app.core.db import init_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    """Create initial database."""
    logger.info("Creating initial data")
    init_db()
    logger.info("Initial data created")


if __name__ == "__main__":
    main()
