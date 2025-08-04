import os
import json
from typing import Optional

import structlog

from crawler.database import connection as db_connection
from crawler.database import repository
from crawler.database.connection import initialize_database
from crawler.database.schemas import SourcePlatform, CategorySourcePydantic
from crawler.project_yes123.client_yes123 import fetch_yes123_category_data
from crawler.project_yes123.config_yes123 import JOB_CAT_URL_YES123
from crawler.worker import app
from crawler.config import MYSQL_DATABASE, get_db_name_for_platform

# Import MAPPING from apply_classification.py
from crawler.database.category_classification_data.apply_classification import MAPPING

logger = structlog.get_logger(__name__)


def flatten_yes123_categories(
    json_content: str, url_JobCat: str
) -> list[CategorySourcePydantic]:
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
                        logger.warning(
                            "Unexpected code format for parent derivation.",
                            event="unexpected_code_format_parent_derivation",
                            code=source_category_id,
                            platform=SourcePlatform.PLATFORM_YES123,
                            component="task",
                        )


                if source_category_id and source_category_name:
                    flattened.append(
                        CategorySourcePydantic(
                            source_platform=SourcePlatform.PLATFORM_YES123,
                            source_category_id=source_category_id,
                            category_name=source_category_name,
                            category_url=f"https://www.yes123.com.tw/findjob/job_list.asp?find_work_mode1={source_category_id}",
                            parent_category_id=parent_source_id,
                        )
                    )
    except json.JSONDecodeError as e:
        logger.error(
            "Failed to decode JSON content for yes123 categories.",
            event="json_decode_error",
            url=url_JobCat,
            error=str(e),
            platform=SourcePlatform.PLATFORM_YES123,
            component="task",
            exc_info=True,
        )
    except Exception as e:
        logger.error(
            "An unexpected error occurred during yes123 category flattening.",
            event="unexpected_flattening_error",
            url=url_JobCat,
            error=str(e),
            platform=SourcePlatform.PLATFORM_YES123,
            component="task",
            exc_info=True,
        )

    return flattened


logger = structlog.get_logger(__name__)


@app.task()
def fetch_and_sync_yes123_categories(url_JobCat: str, db_name_override: Optional[str] = None):
    db_name = db_name_override if db_name_override else get_db_name_for_platform(SourcePlatform.PLATFORM_YES123.value)
    logger.info("Current database connection", db_url=str(db_connection.get_engine(db_name=db_name).url))
    logger.info(
        "Starting yes123 category data fetch and sync.",
        event="start_category_sync",
        url=url_JobCat,
        platform=SourcePlatform.PLATFORM_YES123,
        component="task",
    )

    try:
        existing_categories = repository.get_source_categories(SourcePlatform.PLATFORM_YES123, db_name=db_name)

        html_content = fetch_yes123_category_data(url_JobCat)
        if html_content is None:
            logger.error(
                "Failed to fetch yes123 category data from API.",
                event="fetch_category_data_failed",
                url=url_JobCat,
                error="HTML content is None",
                platform=SourcePlatform.PLATFORM_YES123,
                component="task",
                exc_info=True,
            )
            return

        flattened_data = flatten_yes123_categories(html_content, url_JobCat)
        # Sort flattened_data by source_category_id before initial sync
        flattened_data.sort(key=lambda x: x.source_category_id)

        if not flattened_data:
            logger.warning(
                "No 'jobPosition' data found in yes123 category API response.",
                event="no_job_position_data",
                url=url_JobCat,
                platform=SourcePlatform.PLATFORM_YES123,
                component="task",
            )
            return

        if not existing_categories:
            logger.info("yes123 category database is empty. Performing initial bulk sync.",
                event="initial_bulk_sync",
                total_api_categories=len(flattened_data),
                platform=SourcePlatform.PLATFORM_YES123,
                component="task",
            )
            repository.sync_source_categories(SourcePlatform.PLATFORM_YES123, flattened_data, db_name=db_name)
            return

        api_categories_set = {
            (d.source_category_id, d.source_category_name, d.parent_source_id)
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
                CategorySourcePydantic(
                    source_platform=SourcePlatform.PLATFORM_YES123,
                    source_category_id=cat_id,
                    source_category_name=name,
                    # category_url=f"https://www.yes123.com.tw/findjob/job_list.asp?find_work_mode1={cat_id}", # Removed as it's not part of schema
                    parent_source_id=parent_id,
                )
                for cat_id, name, parent_id in categories_to_sync_set
            ]
            categories_to_sync.sort(key=lambda x: x.source_category_id)
            logger.info("Found new or updated yes123 categories to sync.",
                event="new_categories_to_sync",
                count=len(categories_to_sync),
                platform=SourcePlatform.PLATFORM_YES123,
                component="task",
            )
            repository.sync_source_categories(SourcePlatform.PLATFORM_YES123, categories_to_sync, db_name=db_name)
        else:
            logger.info("No new or updated yes123 categories to sync.",
                event="no_new_categories_to_sync",
                existing_categories_count=len(existing_categories),
                api_categories_count=len(flattened_data),
                platform=SourcePlatform.PLATFORM_YES123,
                component="task",
            )

    except Exception as e:
        logger.error("An unexpected error occurred during yes123 category sync.",
            event="unexpected_category_sync_error",
            error=str(e),
            url=url_JobCat,
            platform=SourcePlatform.PLATFORM_YES123,
            component="task",
            exc_info=True,
        )


if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
    initialize_database()
    logger.info("Dispatching fetch_and_sync_yes123_categories task for local testing.",
        event="dispatching_category_sync_task",
        url=JOB_CAT_URL_YES123,
        platform=SourcePlatform.PLATFORM_YES123,
        component="task",
    )
    fetch_and_sync_yes123_categories(JOB_CAT_URL_YES123)