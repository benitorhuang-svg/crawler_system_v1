import os
import sys
import structlog
from sqlalchemy import text

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from crawler.database.connection import get_session, initialize_database
from crawler.logging_config import configure_logging

# Configure logging at the script level
configure_logging()
logger = structlog.get_logger(__name__)


def main():
    """
    Connects to the database and counts the records in key tables.
    """
    logger.info("Starting database count check.")
    try:
        # Ensure the database and tables exist before counting
        initialize_database()
        
        with get_session() as session:
            category_count = session.execute(
                text("SELECT COUNT(*) FROM tb_category_source")
            ).scalar_one_or_none() or 0
            url_count = session.execute(text("SELECT COUNT(*) FROM tb_urls")).scalar_one_or_none() or 0

            logger.info(
                "Database record counts retrieved successfully.",
                category_count=category_count,
                url_count=url_count,
            )

    except Exception as e:
        logger.error(
            "An error occurred while counting database records.", error=e, exc_info=True
        )

if __name__ == "__main__":
    # To run this script for the test database, set the environment variable:
    # CRAWLER_DB_NAME=test_db python -m crawler.database.scripts.temp_count_db
    main()