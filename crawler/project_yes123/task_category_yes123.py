import os
# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---

import structlog
from bs4 import BeautifulSoup
import re

from crawler.worker import app
from crawler.database import connection as db_connection
from crawler.database import repository
from crawler.database.connection import initialize_database
from crawler.database.schemas import SourcePlatform
from crawler.project_yes123.client_yes123 import fetch_yes123_category_data
from crawler.project_yes123.config_yes123 import JOB_CAT_URL_YES123

logger = structlog.get_logger(__name__)


def flatten_yes123_categories(html_content):
    """
    Parses the HTML content from yes123 category page and flattens the category structure.
    """
    flattened = []
    soup = BeautifulSoup(html_content, 'html.parser')

    category_table = soup.find('table', class_='table_01')

    if not category_table:
        logger.error("Could not find category table in yes123 HTML.")
        return flattened

    for row in category_table.find_all('tr'):
        for td in row.find_all('td'):
            link = td.find('a')
            if link and 'href' in link.attrs:
                href = link['href']
                match = re.search(r'job_kind=(\d+)', href)
                if match:
                    source_category_id = match.group(1)
                    source_category_name = link.get_text(strip=True)
                    
                    parent_source_id = None
                    if len(source_category_id) == 9 and source_category_id.endswith('001'):
                        parent_source_id = source_category_id[:6]
                    elif len(source_category_id) == 6 and source_category_id.endswith('00'):
                        parent_source_id = source_category_id[:3]

                    flattened.append({
                        "parent_source_id": parent_source_id,
                        "source_category_id": source_category_id,
                        "source_category_name": source_category_name,
                        "source_platform": SourcePlatform.PLATFORM_YES123.value,
                    })
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
    # python -m crawler.project_yes123.task_category_yes123
    
    initialize_database()

    logger.info("Dispatching fetch_url_data_yes123 task for local testing.", url=JOB_CAT_URL_YES123)
    fetch_url_data_yes123(JOB_CAT_URL_YES123)