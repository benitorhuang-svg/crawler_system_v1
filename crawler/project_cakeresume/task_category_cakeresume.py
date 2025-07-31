import os
# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---


import structlog

from crawler.worker import app
from crawler.database import connection as db_connection
from crawler.database import repository
from crawler.database.connection import initialize_database
from crawler.database.schemas import SourcePlatform
from crawler.project_cakeresume.client_cakeresume import (
    fetch_cakeresume_category_data,
)
from crawler.project_cakeresume.config_cakeresume import JOB_CAT_URL_CAKERESUME

logger = structlog.get_logger(__name__)


def flatten_cakeresume_categories(data):
    """
    Flattens the CakeResume category data structure.
    """
    flattened = []
    sectors_path = data.get('initialI18nStore', {}).get('zh-TW', {}).get('sector', {}).get('sectors', {})
    for key, name in sectors_path.items():
        source_category_id = key.split('_')[-1] if '_' in key else key.replace('.', '')
        flattened.append({
            "parent_source_id": None,
            "source_category_id": source_category_id,
            "source_category_name": name,
            "source_platform": SourcePlatform.PLATFORM_CAKERESUME.value,
        })

    sector_groups_path = data.get('initialI18nStore', {}).get('zh-TW', {}).get('sector', {}).get('sector_groups', {})
    for group_key, group_data in sector_groups_path.items():
        for sub_key, sub_name in group_data.items():
            source_category_id = sub_key.split('_')[-1] if '_' in sub_key else sub_key.replace('.', '')
            flattened.append({
                "parent_source_id": group_key,
                "source_category_id": source_category_id,
                "source_category_name": sub_name,
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
            if d.get("parent_source_id")
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