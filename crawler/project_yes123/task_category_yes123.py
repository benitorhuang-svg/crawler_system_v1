# import os
# # python -m crawler.project_yes123.task_category_yes123
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import json

import structlog

from crawler.database import connection as db_connection
from crawler.database import repository
from crawler.database.connection import initialize_database
from crawler.database.schemas import SourcePlatform
from crawler.project_yes123.client_yes123 import fetch_yes123_category_data
from crawler.project_yes123.config_yes123 import JOB_CAT_URL_YES123
from crawler.worker import app

# Import MAPPING from apply_classification.py
from crawler.database.category_classification_data.apply_classification import MAPPING

logger = structlog.get_logger(__name__)


def flatten_yes123_categories(json_content):
    """
    Parses the JSON content from yes123 category data and flattens the category structure.
    Applies major category mapping for top-level categories.
    """
    flattened = []
    try:
        data = json.loads(json_content.encode('utf-8').decode('utf-8-sig'))
        list_obj = data.get('listObj', [])

        for level1_item in list_obj:
            for level2_item in level1_item.get('list_2', []):
                source_category_id = level2_item.get('code')
                source_category_name = level2_item.get('level_2_name')

                parent_source_id = None
                if source_category_id:
                    parts = source_category_id.split('_')
                    if len(parts) == 4:
                        if parts[3] != '0000':
                            parent_source_id = f"{parts[0]}_{parts[1]}_{parts[2]}_0000"
                        else: # This is a top-level category in Yes123's original structure
                            mapped_parent_id = MAPPING[SourcePlatform.PLATFORM_YES123].get(source_category_name)
                            if mapped_parent_id:
                                parent_source_id = mapped_parent_id
                    else:
                        logger.warning("Unexpected code format for parent derivation.", code=source_category_id)


                if source_category_id and source_category_name:
                    flattened.append({
                        "parent_source_id": parent_source_id,
                        "source_category_id": source_category_id,
                        "source_category_name": source_category_name,
                        "source_platform": SourcePlatform.PLATFORM_YES123.value,
                    })
    except json.JSONDecodeError as e:
        logger.error("Failed to decode JSON content for yes123 categories.", error=e, exc_info=True)
    except Exception as e:
        logger.error("An unexpected error occurred during yes123 category flattening.", error=e, exc_info=True)

    return flattened


@app.task()
def fetch_url_data_yes123(url_JobCat: str = JOB_CAT_URL_YES123):
    logger.info("Current database connection", db_url=str(db_connection.get_engine().url))
    logger.info("Starting yes123 category data fetch and sync.", url=url_JobCat)

    try:
        existing_categories = repository.get_source_categories(SourcePlatform.PLATFORM_YES123)

        html_content = fetch_yes123_category_data(url_JobCat)
        if html_content is None:
            logger.error("Failed to fetch yes123 category data from web.", url=url_JobCat)
            return

        flattened_data = flatten_yes123_categories(html_content)

        if not flattened_data:
            logger.warning("No categories found in yes123 category data.", url=url_JobCat)
            return

        if not existing_categories:
            logger.info("yes123 category database is empty. Performing initial bulk sync.", total_api_categories=len(flattened_data))
            repository.sync_source_categories(SourcePlatform.PLATFORM_YES123, flattened_data)
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
                    "source_platform": SourcePlatform.PLATFORM_YES123.value,
                }
                for cat_id, name, parent_id in categories_to_sync_set
            ]
            categories_to_sync.sort(key=lambda x: x['source_category_id'])
            logger.info(
                "Found new or updated yes123 categories to sync.",
                count=len(categories_to_sync),
            )
            repository.sync_source_categories(SourcePlatform.PLATFORM_YES123, categories_to_sync)
        else:
            logger.info("No new or updated yes123 categories to sync.", existing_categories_count=len(existing_categories), api_categories_count=len(flattened_data))

    except Exception as e:
        logger.error("An unexpected error occurred during yes123 category sync.", error=e, exc_info=True, url=url_JobCat)


if __name__ == "__main__":
    initialize_database()
    logger.info("Dispatching fetch_url_data_yes123 task for local testing.", url=JOB_CAT_URL_YES123)
    fetch_url_data_yes123(JOB_CAT_URL_YES123)