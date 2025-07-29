import json
import requests
import structlog

from crawler.worker import app
from crawler.logging_config import configure_logging
from crawler.database.models import SourcePlatform
from crawler.database.repository import get_source_categories, sync_source_categories

configure_logging()
logger = structlog.get_logger(__name__)

WEB_NAME = '104_人力銀行'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Referer': 'https://www.104.com.tw/jobs/search',
}

def flatten_jobcat_recursive(node_list, parent_no=None):
    """Recursively flattens the category tree using a generator."""
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


# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def fetch_url_data_104(url_JobCat):
    from crawler.database.connection import initialize_database
    initialize_database()

    try:
        # Fetch existing data from the database first
        existing_categories = get_source_categories(SourcePlatform.PLATFORM_104)

        # Fetch and process API data
        response_jobcat = requests.get(url_JobCat, headers=HEADERS, timeout=10)
        response_jobcat.raise_for_status()
        jobcat_data = response_jobcat.json()
        flattened_data = list(flatten_jobcat_recursive(jobcat_data))

        # If the database is empty, do a fast bulk insert
        if not existing_categories:
            logger.info("Database is empty. Performing initial bulk sync.")
            sync_source_categories(SourcePlatform.PLATFORM_104, flattened_data)
            return

        # Otherwise, perform an intelligent sync using set operations
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
        logger.error(f"Error fetching data from {url_JobCat}: {e}", exc_info=True)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {url_JobCat}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    # 啟動本地測試 task_category_104
    # APP_ENV=DEV python -m crawler.project_104.task_category_104
    JobCat_url_104 = "https://static.104.com.tw/category-tool/json/JobCat.json"
    fetch_url_data_104(JobCat_url_104)