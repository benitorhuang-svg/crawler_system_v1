import os
import sys
import structlog
import pandas as pd

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from crawler.database.connection import get_session, initialize_database
from crawler.database.models import CategorySource
from crawler.logging_config import configure_logging

# Configure logging at the script level
configure_logging()
logger = structlog.get_logger(__name__)


def get_source_category_ids():
    """
    Fetches all category IDs and names from the database and returns them as a Pandas DataFrame.
    """
    try:
        with get_session() as session:
            categories = session.query(CategorySource).all()
            data = [
                {
                    "parent_source_id": cat.parent_source_id,
                    "source_category_id": cat.source_category_id,
                    "source_category_name": cat.source_category_name,
                }
                for cat in categories
            ]
            df = pd.DataFrame(data)
            logger.info("Successfully fetched source category IDs.", count=len(df))
            return df
    except Exception as e:
        logger.error("Error fetching source_category_ids with ORM.", error=e, exc_info=True)
        return pd.DataFrame()  # Return an empty DataFrame on error


if __name__ == "__main__":
    # To run this script for the test database, set the environment variable:
    # CRAWLER_DB_NAME=test_db python -m crawler.database.scripts.get_category_ids
    
    # Ensure the database is initialized before fetching data
    initialize_database()

    ids_df = get_source_category_ids()
    if not ids_df.empty:
        logger.info(
            "Source Category IDs fetched successfully.",
            dataframe_head=ids_df.head().to_dict("records")
        )
    else:
        logger.warning("Could not fetch any source category IDs.")