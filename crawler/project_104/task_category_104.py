import structlog

from crawler.worker import app
from crawler.database.models import SourcePlatform
from crawler.database.repository import get_source_categories, sync_source_categories
from crawler.project_104.config_104 import HEADERS_104  # Changed import path
from crawler.project_104.client_104 import (
    fetch_category_data_from_104_api,
)  # Import the new API client

logger = structlog.get_logger(__name__)


def flatten_jobcat_recursive(node_list, parent_no=None):
    """
    Recursively flattens the category tree using a generator.
    """
    for node in node_list:
        yield {
            "parent_source_id": parent_no,
            "source_category_id": node.get("no"),
            "source_category_name": node.get("des"),
        }
        if "n" in node and node.get("n"):
            yield from flatten_jobcat_recursive(
                node_list=node["n"],
                parent_no=node.get("no"),
            )


@app.task()
def fetch_url_data_104(url_JobCat):
    logger.info("Starting category data fetch and sync.", url=url_JobCat)

    try:
        existing_categories = get_source_categories(SourcePlatform.PLATFORM_104)

        # 使用新的 API 客戶端模組來獲取數據
        jobcat_data = fetch_category_data_from_104_api(url_JobCat, HEADERS_104)
        if jobcat_data is None:
            logger.error("Failed to fetch category data from 104 API.", url=url_JobCat)
            return

        flattened_data = list(flatten_jobcat_recursive(jobcat_data))

        if not existing_categories:
            logger.info("Database is empty. Performing initial bulk sync.", total_api_categories=len(flattened_data))
            sync_source_categories(SourcePlatform.PLATFORM_104, flattened_data)
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
                    "source_platform": SourcePlatform.PLATFORM_104.value,
                }
                for cat_id, name, parent_id in categories_to_sync_set
            ]
            logger.info(
                "Found new or updated categories to sync.",
                count=len(categories_to_sync),
            )
            sync_source_categories(SourcePlatform.PLATFORM_104, categories_to_sync)
        else:
            logger.info("No new or updated categories to sync.", existing_categories_count=len(existing_categories), api_categories_count=len(flattened_data))

    except Exception as e:
        logger.error("An unexpected error occurred during category sync.", error=e, exc_info=True, url=url_JobCat)


if __name__ == "__main__":
    from crawler.database.connection import initialize_database
    from crawler.project_104.config_104 import JOB_CAT_URL_104
    initialize_database()
    logger.info("Dispatching fetch_url_data_104 task for local testing.", url=JOB_CAT_URL_104)
    fetch_url_data_104(JOB_CAT_URL_104)
