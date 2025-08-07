import os

# # python -m crawler.project_104.task_category_104
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---


from typing import Optional
import structlog

from crawler.database import connection as db_connection
from crawler.database import repository
from crawler.database.connection import initialize_database
from crawler.database.schemas import SourcePlatform
from crawler.project_104.client_104 import fetch_category_data_from_104_api
from crawler.project_104.config_104 import HEADERS_104, JOB_CAT_URL_104
from crawler.worker import app
from crawler.config import MYSQL_DATABASE, get_db_name_for_platform

# Import MAPPING from apply_classification.py
from crawler.database.category_classification_data.apply_classification import MAPPING

logger = structlog.get_logger(__name__)


def flatten_jobcat_recursive(node_list, parent_no=None):
    """
    Recursively flattens the category tree using a generator.
    Applies major category mapping for top-level categories.
    """
    for node in node_list:
        current_parent_id = parent_no
        if parent_no is None: # Only apply mapping for top-level categories
            category_name = node.get("des")
            mapped_parent_id = MAPPING[SourcePlatform.PLATFORM_104].get(category_name)
            if mapped_parent_id:
                current_parent_id = mapped_parent_id

        yield {
            "parent_source_id": current_parent_id,
            "source_category_id": node.get("no"),
            "source_category_name": node.get("des"),
            "source_platform": SourcePlatform.PLATFORM_104.value,
        }
        if "n" in node and node.get("n"):
            yield from flatten_jobcat_recursive(
                node_list=node["n"],
                parent_no=node.get("no"),
            )


@app.task()
def fetch_url_data_104(url_JobCat, db_name_override: Optional[str] = None):
    db_name = get_db_name_for_platform(SourcePlatform.PLATFORM_104.value)
    if db_name_override:
        db_name = db_name_override
    elif os.environ.get('CRAWLER_DB_NAME'):
        db_name = MYSQL_DATABASE
    
    initialize_database(db_name=db_name) # Ensure database is initialized before any operations

    logger.info("Current database connection", db_url=str(db_connection.get_engine(db_name=db_name).url))
    logger.info("Starting category data fetch and sync.", url=url_JobCat)

    try:
        existing_categories = repository.get_source_categories(SourcePlatform.PLATFORM_104, db_name=db_name)

        jobcat_data = fetch_category_data_from_104_api(url_JobCat, HEADERS_104)
        if jobcat_data is None:
            logger.error("Failed to fetch category data from 104 API.", url=url_JobCat)
            return

        flattened_data = list(flatten_jobcat_recursive(jobcat_data))
        # Sort flattened_data by source_category_id before initial sync
        flattened_data.sort(key=lambda x: x['source_category_id'])

        if not existing_categories:
            logger.info("Database is empty. Performing initial bulk sync.", total_api_categories=len(flattened_data))
            repository.sync_source_categories(SourcePlatform.PLATFORM_104, flattened_data, db_name=db_name)
            return

        api_categories_set = {
            (d["source_category_id"], d["source_category_name"], d["parent_source_id"])
            for d in flattened_data
        }
        db_categories_set = {
            (
                category.source_category_id,
                category.source_category_name,
                category.parent_source_id,
            )
            for category in existing_categories
        }

        categories_to_sync_set = api_categories_set - db_categories_set

        if categories_to_sync_set:
            categories_to_sync = [
                {
                    "source_category_id": cat_id,
                    "source_category_name": name,
                    "parent_source_id": parent_id,
                    "source_platform": SourcePlatform.PLATFORM_104.value,
                }
                for cat_id, name, parent_id in categories_to_sync_set
            ]
            categories_to_sync.sort(key=lambda x: x['source_category_id'])
            logger.info(
                "Found new or updated categories to sync.",
                count=len(categories_to_sync),
            )
            repository.sync_source_categories(SourcePlatform.PLATFORM_104, categories_to_sync, db_name=db_name)
        else:
            logger.info("No new or updated categories to sync.", existing_categories_count=len(existing_categories), api_categories_count=len(flattened_data))

    except Exception as e:
        logger.error("An unexpected error occurred during category sync.", error=e, exc_info=True, url=url_JobCat)


if __name__ == "__main__":
    initialize_database()
    actual_db_name_for_logging = MYSQL_DATABASE if os.environ.get('CRAWLER_DB_NAME') else get_db_name_for_platform(SourcePlatform.PLATFORM_104.value)
    logger.info("Dispatching fetch_url_data_104 task for local testing.", url=JOB_CAT_URL_104, db_name=actual_db_name_for_logging)
    fetch_url_data_104(JOB_CAT_URL_104)