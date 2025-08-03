
import sys
import os
import structlog
from sqlalchemy import text

# This is a bit of a hack to make the script runnable from the root directory
# while being able to import modules from the 'crawler' subdirectory.
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from crawler.database.connection import get_session, get_db_name
from crawler.logging_config import configure_logging

# Configure logging
configure_logging()
logger = structlog.get_logger(__name__)

def copy_data_to_platform_tables():
    """
    Copies data from the main 'tb_jobs' table to platform-specific tables
    (e.g., 'tb_jobs_104', 'tb_jobs_1111').
    """
    platforms = {
        "PLATFORM_104": "tb_jobs_104",
        "PLATFORM_1111": "tb_jobs_1111",
        "PLATFORM_YES123": "tb_jobs_yes123",
        "PLATFORM_CAKERESUME": "tb_jobs_cakeresume",
    }

    db_name = get_db_name()
    logger.info(f"Starting data migration for database: '{db_name}'")

    try:
        with get_session() as session:
            for platform_name, target_table in platforms.items():
                logger.info(f"Copying data for '{platform_name}' to '{target_table}'...")

                # Using parameterized query to prevent SQL injection
                sql_query = text(
                    f"INSERT INTO {target_table} SELECT * FROM tb_jobs WHERE source_platform = :platform"
                )

                try:
                    result = session.execute(sql_query, {"platform": platform_name})
                    # A commit is issued by the context manager at the end of the 'with' block.
                    # If we want to commit after each insert, we can call session.commit() here.
                    logger.info(
                        f"Successfully copied data for '{platform_name}'.",
                        rows_affected=result.rowcount,
                    )
                except Exception as e:
                    logger.error(
                        f"Could not copy data for '{platform_name}'. An error occurred.",
                        table=target_table,
                        error=str(e),
                        exc_info=True,
                    )
                    # The session will be rolled back by the context manager on exception.
                    # We can re-raise to stop the whole process or continue with other platforms.
                    # For now, let's log the error and continue.
                    pass # Continue to the next platform

    except Exception as e:
        logger.critical(
            "A critical error occurred during the database session setup.",
            error=str(e),
            exc_info=True,
        )
        sys.exit(1)

    logger.info("Data migration process finished.")

if __name__ == "__main__":
    copy_data_to_platform_tables()
