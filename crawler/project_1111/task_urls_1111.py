import os
# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---

import structlog
from collections import deque
from crawler.worker import app
from crawler.database.schemas import (
    SourcePlatform,
    UrlCategoryPydantic,
    CategorySourcePydantic,
)
from crawler.database.repository import (
    upsert_urls,
    upsert_url_categories,
    upsert_jobs,
    get_all_categories_for_platform,
)
from crawler.project_1111.client_1111 import fetch_job_urls_from_1111_api
from crawler.project_1111.parser_apidata_1111 import parse_job_list_json_to_pydantic
from crawler.config import (
    URL_CRAWLER_UPLOAD_BATCH_SIZE,
)
from crawler.project_1111.config_1111 import (
    URL_CRAWLER_ORDER_BY_1111,
)

logger = structlog.get_logger(__name__)


@app.task
def crawl_and_store_1111_category_urls(job_category: dict, url_limit: int = 0) -> int:
    """
    Celery task: Iterates through all pages of a specified 1111 job category, fetches job URLs
    and preliminary data, and stores them in the database in batches.
    """
    job_category = CategorySourcePydantic.model_validate(job_category)
    job_category_code = job_category.source_category_id
    
    global_job_url_set = set()
    current_batch_jobs = []
    current_batch_urls = []
    current_batch_url_categories = []
    recent_counts = deque(maxlen=4)

    current_page = 1
    logger.info(
        "Task started: crawling 1111 job category URLs and data.",
        job_category_code=job_category_code,
        url_limit=url_limit,
    )

    while True:
        if url_limit > 0 and len(global_job_url_set) >= url_limit:
            logger.info(
                "URL limit reached. Ending task early.",
                job_category_code=job_category_code,
                url_limit=url_limit,
                collected_urls=len(global_job_url_set),
            )
            break

        if current_page % 5 == 1:
            logger.info(
                "Current page being processed.",
                page=current_page,
                job_category_code=job_category_code,
            )

        api_response = fetch_job_urls_from_1111_api(
            KEYWORDS="",
            CATEGORY=job_category_code,
            ORDER=URL_CRAWLER_ORDER_BY_1111,
            PAGE_NUM=current_page,
        )

        if api_response is None:
            logger.error(
                "Failed to retrieve data from 1111 API.",
                page=current_page,
                job_category_code=job_category_code,
            )
            break

        job_items = api_response.get("result", {}).get("hits", [])
        if not isinstance(job_items, list):
            logger.error(
                "API response 'result.hits' format is incorrect or missing.",
                page=current_page,
                job_category_code=job_category_code,
                api_data_type=type(job_items),
            )
            break

        if not job_items:
            logger.info(
                "No more job items found. Ending task.",
                page=current_page,
                job_category_code=job_category_code,
            )
            break

        for job_item in job_items:
            job_pydantic = parse_job_list_json_to_pydantic(job_item)
            if job_pydantic and job_pydantic.url:
                if job_pydantic.url not in global_job_url_set:
                    global_job_url_set.add(job_pydantic.url)
                    current_batch_jobs.append(job_pydantic)
                    current_batch_urls.append(job_pydantic.url)
                
                current_batch_url_categories.append(
                    UrlCategoryPydantic(
                        source_url=job_pydantic.url,
                        source_category_id=job_category_code,
                    ).model_dump()
                )

        if len(current_batch_urls) >= URL_CRAWLER_UPLOAD_BATCH_SIZE:
            logger.info(
                "Batch upload size reached. Starting data upload.",
                count=len(current_batch_urls),
                job_category_code=job_category_code,
            )
            upsert_jobs(current_batch_jobs)
            upsert_urls(SourcePlatform.PLATFORM_1111, current_batch_urls)
            upsert_url_categories(current_batch_url_categories)
            current_batch_jobs.clear()
            current_batch_urls.clear()
            current_batch_url_categories.clear()

        total_jobs = len(global_job_url_set)
        recent_counts.append(total_jobs)
        if len(recent_counts) == recent_counts.maxlen and len(set(recent_counts)) == 1:
            logger.info(
                "No new data found consecutively. Ending task early.",
                job_category_code=job_category_code,
            )
            break

        current_page += 1

    if current_batch_urls:
        logger.info(
            "Task completed. Storing remaining data to database.",
            count=len(current_batch_urls),
            job_category_code=job_category_code,
        )
        upsert_jobs(current_batch_jobs)
        upsert_urls(SourcePlatform.PLATFORM_1111, current_batch_urls)
        upsert_url_categories(current_batch_url_categories)
    else:
        logger.info(
            "Task completed. No new data collected, skipping database storage.",
            job_category_code=job_category_code,
        )

    logger.info("Task execution finished.", job_category_code=job_category_code)
    return len(global_job_url_set)


if __name__ == "__main__":
    # python -m crawler.project_1111.task_urls_1111
    
    # --- Database Initialization for Local Test ---
    from crawler.database.connection import initialize_database
    initialize_database()
    # --- End Database Initialization ---

    job_category_lists = get_all_categories_for_platform(SourcePlatform.PLATFORM_1111)
    
    # Only process the first category for local testing
    if job_category_lists:
        test_category = job_category_lists[1] # Changed from [1] to [0]
        
        logger.info("Testing with category:", job_category=test_category.model_dump())

        url_limit = 20
        total_urls_collected = crawl_and_store_1111_category_urls(test_category.model_dump(), url_limit=url_limit) # Changed url_limit to 20
        
        logger.info("Total URLs collected:", count=total_urls_collected)
    else:
        logger.info("No categories found to process for testing.")
