import json
import requests
import structlog

from crawler.worker import app
# from crawler.logging_config import configure_logging # Removed this import
from crawler.database.models import SourcePlatform
from crawler.database.repository import get_source_categories, sync_source_categories
from crawler.project_104.config_104 import HEADERS_104, JOB_CAT_URL_104 # Changed import path

# configure_logging() # Removed this call
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
    logger.info("Fetching category data", url=url_JobCat)

    try:
        existing_categories = get_source_categories(SourcePlatform.PLATFORM_104)

        response_jobcat = requests.get(url_JobCat, headers=HEADERS_104, timeout=10)
        response_jobcat.raise_for_status()
        jobcat_data = response_jobcat.json()
        flattened_data = list(flatten_jobcat_recursive(jobcat_data))

        if not existing_categories:
            logger.info("Database is empty. Performing initial bulk sync.")
            sync_source_categories(SourcePlatform.PLATFORM_104, flattened_data)
            return

        api_categories_set = {
            (d["source_category_id"], d["source_category_name"], d["parent_source_id"])
            for d in flattened_data if d.get("parent_source_id")
        }
        db_categories_set = {
            (category.source_category_id, category.source_category_name, category.parent_source_id)
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
            logger.info("Found new or updated categories to sync.", count=len(categories_to_sync))
            sync_source_categories(SourcePlatform.PLATFORM_104, categories_to_sync)
        else:
            logger.info("No new or updated categories to sync.")

    except requests.exceptions.RequestException as e:
        logger.error("Error fetching data from URL.", url=url_JobCat, error=e, exc_info=True)
    except json.JSONDecodeError as e:
        logger.error("Error decoding JSON from URL.", url=url_JobCat, error=e, exc_info=True)
    except Exception as e:
        logger.error("An unexpected error occurred.", error=e, exc_info=True)

# if __name__ == "__main__":
#     logger.info("Dispatching fetch_url_data_104 task for local testing.")
#     fetch_url_data_104.delay(JOB_CAT_URL_104)