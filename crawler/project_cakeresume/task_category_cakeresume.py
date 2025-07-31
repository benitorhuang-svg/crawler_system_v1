# import os
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---


import structlog

from crawler.database import connection as db_connection
from crawler.database import repository
from crawler.database.connection import initialize_database
from crawler.database.schemas import SourcePlatform
from crawler.project_cakeresume.client_cakeresume import (
    fetch_cakeresume_category_data,
)
from crawler.project_cakeresume.config_cakeresume import JOB_CAT_URL_CAKERESUME
from crawler.worker import app

# Import MAPPING from apply_classification.py
from crawler.database.category_classification_data.apply_classification import MAPPING

logger = structlog.get_logger(__name__)


def flatten_cakeresume_categories(data):
    """
    Flattens the CakeResume category data structure and applies major category mapping.
    """
    flattened = []
    
    # Get the 'sector' dictionary directly
    sector_data = data.get('initialI18nStore', {}).get('zh-TW', {}).get('sector', {})

    for key, name in sector_data.items():
        if key.startswith('sector_groups.'):
            # This is a top-level group from Cakeresume, apply major category mapping
            source_category_id = key.replace('sector_groups.', '')
            
            # Determine parent_source_id based on major category mapping from MAPPING
            mapped_parent_id = MAPPING[SourcePlatform.PLATFORM_CAKERESUME].get(name)
            
            flattened.append({
                "parent_source_id": mapped_parent_id, # Use mapped ID or None
                "source_category_id": source_category_id,
                "source_category_name": name,
                "source_platform": SourcePlatform.PLATFORM_CAKERESUME.value,
            })
        elif key.startswith('sectors.'):
            # This is a sub-category, preserve Cakeresume's internal hierarchy
            full_category_id = key.replace('sectors.', '') # e.g., "advertising-marketing-agency_adtech-martech"
            parts = full_category_id.split('_', 1)
            if len(parts) > 1:
                parent_source_id = parts[0] # e.g., "advertising-marketing-agency"
                source_category_id = full_category_id # The full key is the source_category_id
            else:
                # Handle cases where 'sectors.' key might not have an underscore for parent
                # This might be a top-level sector if it doesn't have a group parent
                parent_source_id = None # Keep as None if no internal parent
                source_category_id = full_category_id

            flattened.append({
                "parent_source_id": parent_source_id,
                "source_category_id": source_category_id,
                "source_category_name": name,
                "source_platform": SourcePlatform.PLATFORM_CAKERESUME.value,
            })
    return flattened


@app.task()
def fetch_url_data_cakeresume(url_JobCat: str = JOB_CAT_URL_CAKERESUME):
    logger.info("Current database connection", db_url=str(db_connection.get_engine().url))
    logger.info("Starting CakeResume category data fetch and sync.", url=url_JobCat)

    try:
        existing_categories = repository.get_source_categories(SourcePlatform.PLATFORM_CAKERESUME)

        jobcat_data = fetch_cakeresume_category_data(url_JobCat)
        if jobcat_data is None:
            logger.error("Failed to fetch CakeResume category data from web.", url=url_JobCat)
            return

        flattened_data = flatten_cakeresume_categories(jobcat_data)

        if not existing_categories:
            logger.info("CakeResume category database is empty. Performing initial bulk sync.", total_api_categories=len(flattened_data))
            repository.sync_source_categories(SourcePlatform.PLATFORM_CAKERESUME, flattened_data)
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
                    "source_platform": SourcePlatform.PLATFORM_CAKERESUME.value,
                }
                for cat_id, name, parent_id in categories_to_sync_set
            ]
            categories_to_sync.sort(key=lambda x: x['source_category_id'])
            logger.info(
                "Found new or updated CakeResume categories to sync.",
                count=len(categories_to_sync),
            )
            repository.sync_source_categories(SourcePlatform.PLATFORM_CAKERESUME, categories_to_sync)
        else:
            logger.info("No new or updated CakeResume categories to sync.", existing_categories_count=len(existing_categories), api_categories_count=len(flattened_data))

    except Exception as e:
        logger.error("An unexpected error occurred during CakeResume category sync.", error=e, exc_info=True, url=url_JobCat)


if __name__ == "__main__":
    # python -m crawler.project_cakeresume.task_category_cakeresume
    
    initialize_database()

    logger.info("Dispatching fetch_url_data_cakeresume task for local testing.", url=JOB_CAT_URL_CAKERESUME)
    fetch_url_data_cakeresume(JOB_CAT_URL_CAKERESUME)