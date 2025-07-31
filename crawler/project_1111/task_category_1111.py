# import os
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import structlog
from crawler.worker import app
from crawler.database.schemas import SourcePlatform
from crawler.project_1111.config_1111 import HEADERS_1111, JOB_CAT_URL_1111
from crawler.project_1111.client_1111 import fetch_category_data_from_1111_api

logger = structlog.get_logger(__name__)


def flatten_jobcat_recursive(node_list):
    """
    Flattens the 1111 job categories list, extracting main/sub categories.
    """
    for node in node_list:
        yield {
            "parent_source_id": str(node.get("parentCode")) if node.get("parentCode") else None,
            "source_category_id": str(node.get("code")),
            "source_category_name": node.get("name"),
            "source_platform": SourcePlatform.PLATFORM_1111.value,
        }


@app.task()
def fetch_and_sync_1111_categories(url_JobCat: str = JOB_CAT_URL_1111):
    import crawler.database.repository as repository
    logger.info("Starting 1111 category data fetch and sync.", url=url_JobCat)

    try:
        existing_categories = repository.get_source_categories(SourcePlatform.PLATFORM_1111)

        jobcat_data = fetch_category_data_from_1111_api(url_JobCat, HEADERS_1111)
        if jobcat_data is None:
            logger.error("Failed to fetch 1111 category data from API.", url=url_JobCat)
            return

        job_position_data = jobcat_data.get("jobPosition", [])
        if not job_position_data:
            logger.warning("No 'jobPosition' data found in 1111 category API response.", url=url_JobCat)
            return

        flattened_data = list(flatten_jobcat_recursive(job_position_data))

        if not existing_categories:
            logger.info("1111 category database is empty. Performing initial bulk sync.", total_api_categories=len(flattened_data))
            repository.sync_source_categories(SourcePlatform.PLATFORM_1111, flattened_data)
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
                    "source_platform": SourcePlatform.PLATFORM_1111.value,
                }
                for cat_id, name, parent_id in categories_to_sync_set
            ]
            logger.info(
                "Found new or updated 1111 categories to sync.",
                count=len(categories_to_sync),
            )
            repository.sync_source_categories(SourcePlatform.PLATFORM_1111, categories_to_sync)
        else:
            logger.info("No new or updated 1111 categories to sync.", existing_categories_count=len(existing_categories), api_categories_count=len(flattened_data))

    except Exception as e:
        logger.error("An unexpected error occurred during 1111 category sync.", error=e, exc_info=True, url=url_JobCat)


if __name__ == "__main__":
    # python -m crawler.project_1111.task_category_1111
    
    # --- Database Initialization for Local Test ---
    from crawler.database.connection import initialize_database
    initialize_database()
    # --- End Database Initialization ---

    logger.info("Dispatching fetch_and_sync_1111_categories task for local testing.", url=JOB_CAT_URL_1111)
    fetch_and_sync_1111_categories(JOB_CAT_URL_1111)
