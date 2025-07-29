import structlog
import os
from sqlalchemy import text

from crawler.database.connection import get_session  # Use get_session
from crawler.logging_config import configure_logging  # Import configure_logging

configure_logging()  # Call configure_logging at the beginning
logger = structlog.get_logger(__name__)

# Set APP_ENV for local testing (if not already set by environment)
os.environ.setdefault(
    "APP_ENV", "DEV"
)  # Use setdefault to avoid overwriting if already set

logger.info("Starting database count check.")

try:
    with get_session() as session:
        category_count = session.execute(
            text("SELECT COUNT(*) FROM tb_category_source")
        ).scalar()
        url_count = session.execute(text("SELECT COUNT(*) FROM tb_urls")).scalar()

        logger.info(
            "Database counts.", category_count=category_count, url_count=url_count
        )
except Exception as e:
    logger.error(
        "An error occurred while counting database records.", error=e, exc_info=True
    )
