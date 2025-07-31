import os
import sys
import structlog
from sqlalchemy import select
from sqlalchemy.sql import null

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from crawler.database.connection import get_session, initialize_database
from crawler.database.models import CategorySource
from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

def check_null_parent_categories():
    os.environ['CRAWLER_DB_NAME'] = 'test_db' # Ensure we are using the test_db
    initialize_database() # Ensure database is initialized

    logger.info("Checking for categories with NULL parent_source_id in test_db...")
    
    null_parent_categories = []
    with get_session() as session:
        stmt = select(CategorySource).where(CategorySource.parent_source_id == null())
        categories = session.scalars(stmt).all()
        
        for cat in categories:
            null_parent_categories.append({
                "source_platform": cat.source_platform.value,
                "source_category_id": cat.source_category_id,
                "source_category_name": cat.source_category_name
            })
    
    if null_parent_categories:
        logger.info("Found categories with NULL parent_source_id:", count=len(null_parent_categories))
        for cat in null_parent_categories:
            logger.info(
                "Category details",
                platform=cat["source_platform"],
                id=cat["source_category_id"],
                name=cat["source_category_name"]
            )
    else:
        logger.info("No categories found with NULL parent_source_id.")

if __name__ == "__main__":
    check_null_parent_categories()
