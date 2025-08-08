
# python -m crawler.project_cakeresume.task_category_cakeresume
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---


import structlog
import json
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup

from crawler.database import connection as db_connection
from crawler.database import repository
from crawler.database.connection import initialize_database
from crawler.database.schemas import SourcePlatform
from crawler.project_cakeresume.client_cakeresume import (
    fetch_cakeresume_category_page_html,
)
from crawler.project_cakeresume.config_cakeresume import JOB_CAT_URL_CAKERESUME
from crawler.worker import app
from crawler.database.category_classification_data.apply_classification import MAPPING # Changed from MAJOR_CATEGORIES to MAPPING
from crawler.config import get_db_name_for_platform

logger = structlog.get_logger(__name__)


def parse_next_data_for_i18n_categories(html_content: str) -> List[Dict[str, Any]]:
    """
    Finds the __NEXT_DATA__ script tag, parses its JSON content,
    and extracts the hierarchical category data from the i18n (internationalization) object.
    This is the most reliable method.
    Applies major category mapping for top-level categories.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    next_data_script = soup.find('script', id='__NEXT_DATA__')
    
    if not next_data_script or not hasattr(next_data_script, 'string') or not next_data_script.string:
        raise ValueError("Could not find __NEXT_DATA__ script tag or it is empty in the HTML.")

    try:
        data = json.loads(next_data_script.string)
    except json.JSONDecodeError:
        raise ValueError("Failed to parse JSON from __NEXT_DATA__ script tag.")

    try:
        # The definitive path to the translation data
        i18n_data = data['props']['pageProps']['_nextI18Next']['initialI18nStore']['zh-TW']['profession']
    except KeyError as e:
        raise ValueError(f"Unexpected JSON structure in __NEXT_DATA__. Missing key: {e}")

    flat_list = []
    parent_map = {}

    # First pass: Get all parent categories (e.g., "profession_groups.it": "軟體")
    for key, value in i18n_data.items():
        if key.startswith("profession_groups."):
            parent_id = key.replace("profession_groups.", "")
            parent_name = value
            parent_map[parent_id] = parent_name

            mapped_parent_id = MAPPING[SourcePlatform.PLATFORM_CAKERESUME].get(parent_name) # Apply mapping here
            
            flat_list.append({
                "source_platform": SourcePlatform.PLATFORM_CAKERESUME.value,
                "source_category_id": parent_id,
                "source_category_name": parent_name,
                "parent_source_id": mapped_parent_id, # Use mapped_parent_id
            })

    # Second pass: Get all sub-categories and link them to parents
    for key, value in i18n_data.items():
        if key.startswith("professions."):
            full_id = key.replace("professions.", "")
            parts = full_id.split('_', 1)
            if len(parts) > 1:
                parent_id = parts[0]
                if parent_id in parent_map:
                    flat_list.append({
                        "source_platform": SourcePlatform.PLATFORM_CAKERESUME.value,
                        "source_category_id": full_id,
                        "source_category_name": value,
                        "parent_source_id": parent_id,
                    })
                else:
                    logger.warning(f"Found orphan sub-category '{full_id}' with no matching parent '{parent_id}'. Skipping.")

    logger.info("Successfully extracted categories from __NEXT_DATA__.", count=len(flat_list))
    return flat_list


@app.task()
def fetch_url_data_cakeresume(url_JobCat: str = JOB_CAT_URL_CAKERESUME, db_name_override: Optional[str] = None):
    db_name = db_name_override if db_name_override else get_db_name_for_platform(SourcePlatform.PLATFORM_CAKERESUME.value)
    logger.info("Current database connection", db_url=str(db_connection.get_engine(db_name=db_name).url))
    logger.info("Starting CakeResume profession category data fetch and sync.", url=url_JobCat)

    try:
        # Removed: repository.sync_source_categories(SourcePlatform.PLATFORM_104, MAJOR_CATEGORIES, db_name=db_name)
        # MAPPING is now applied within parse_next_data_for_i18n_categories

        html_content = fetch_cakeresume_category_page_html(url_JobCat)
        profession_data: List[Dict[str, Any]] = [] # Initialize profession_data here
        if html_content is None:
            logger.error("Failed to fetch CakeResume category page HTML for profession data.", url=url_JobCat)
        else:
            try:
                profession_data = parse_next_data_for_i18n_categories(html_content)
                # Sort profession_data by source_category_id before initial sync
                profession_data.sort(key=lambda x: x['source_category_id'])
            except ValueError as e:
                logger.error("Failed to parse CakeResume profession category data from HTML.", error=e, exc_info=True, url=url_JobCat)
        
        existing_categories = repository.get_source_categories(SourcePlatform.PLATFORM_CAKERESUME, db_name=db_name)

        api_categories_set = {
            (d["source_category_id"], d["source_category_name"], d["parent_source_id"])
            for d in profession_data
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
                "Found new or updated CakeResume profession categories to sync.",
                count=len(categories_to_sync),
            )
            repository.sync_source_categories(SourcePlatform.PLATFORM_CAKERESUME, categories_to_sync, db_name=db_name)
        else:
            logger.info("No new or updated CakeResume profession categories to sync.", existing_categories_count=len(existing_categories), api_categories_count=len(profession_data))

    except Exception as e:
        logger.error("An unexpected error occurred during CakeResume category sync.", error=e, exc_info=True, url=url_JobCat)


if __name__ == "__main__":
    db_name_for_local_run = "db_cakeresume" # Explicitly set for local testing

    # Ensure the target database and its tables are created before running the task.
    initialize_database(db_name=db_name_for_local_run)

    logger.info(
        "Dispatching fetch_url_data_cakeresume task for local testing.",
        url=JOB_CAT_URL_CAKERESUME,
        db_name=db_name_for_local_run
    )
    
    # Execute the main task function with the determined database name.
    fetch_url_data_cakeresume(
        url_JobCat=JOB_CAT_URL_CAKERESUME, db_name_override=db_name_for_local_run
    )