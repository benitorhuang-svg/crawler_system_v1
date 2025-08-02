# import os
# # python -m crawler.project_104.task_urls_104
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---


import structlog
from collections import deque
from typing import Set, List
from crawler.worker import app
from crawler.database.schemas import (
    SourcePlatform,
    UrlCategoryPydantic,
    CategorySourcePydantic,
)
from crawler.database.connection import initialize_database
from crawler.database.repository import (
    upsert_urls,
    upsert_url_categories,
    upsert_jobs,
    get_all_categories_for_platform,
    get_all_crawled_category_ids_pandas,
    get_stale_crawled_category_ids_pandas,
)
from crawler.project_104.client_104 import fetch_job_urls_from_104_api
from crawler.project_104.parser_apidata_104 import parse_job_item_to_pydantic
from crawler.config import (
    URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
    URL_CRAWLER_UPLOAD_BATCH_SIZE,
)
from crawler.project_104.config_104 import (
    URL_CRAWLER_BASE_URL_104,
    URL_CRAWLER_PAGE_SIZE_104,
    URL_CRAWLER_ORDER_BY_104,
    HEADERS_104_URL_CRAWLER,
)

logger = structlog.get_logger(__name__)


@app.task
def crawl_and_store_category_urls(job_category: dict, url_limit: int = 0) -> None:
    """
    Celery task: Iterates through all pages of a specified job category, fetches job URLs
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
        "Task started: crawling job category URLs and data.",
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

        params = {
            "jobsource": "index_s",
            "page": current_page,
            "pagesize": URL_CRAWLER_PAGE_SIZE_104,
            "order": URL_CRAWLER_ORDER_BY_104,
            "jobcat": job_category_code,
            "mode": "s",
            "searchJobs": "1",
        }

        api_response = fetch_job_urls_from_104_api(
            URL_CRAWLER_BASE_URL_104,
            HEADERS_104_URL_CRAWLER,
            params,
            URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
            verify=False,
        )

        if api_response is None:
            logger.error(
                "Failed to retrieve data from 104 API.",
                page=current_page,
                job_category_code=job_category_code,
            )
            break

        job_items = api_response.get("data", [])
        if not isinstance(job_items, list):
            logger.error(
                "API response 'data.list' format is incorrect or missing.",
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
            job_pydantic = parse_job_item_to_pydantic(job_item)
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
            upsert_urls(SourcePlatform.PLATFORM_104, current_batch_urls)
            upsert_url_categories(current_batch_url_categories)
            
            current_batch_jobs.clear()
            current_batch_urls.clear()
            current_batch_url_categories.clear()

        total_jobs = len(global_job_url_set)
        recent_counts.append(total_jobs)
        if len(recent_counts) == recent_counts.maxlen and len(set(recent_counts)) == 3:
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
        upsert_urls(SourcePlatform.PLATFORM_104, current_batch_urls)
        upsert_url_categories(current_batch_url_categories)
    else:
        logger.info(
            "Task completed. No new data collected, skipping database storage.",
            job_category_code=job_category_code,
        )

    logger.info("Task execution finished.", job_category_code=job_category_code)


if __name__ == "__main__":
    initialize_database()

    n_days = 7  # Define n_days for local testing
    url_limit = 1000000

    all_categories_pydantic: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_104)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories_pydantic}
    all_crawled_category_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_104)
    stale_crawled_category_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_104, n_days)
    categories_to_dispatch_ids = (all_category_ids - all_crawled_category_ids) | stale_crawled_category_ids
    categories_to_dispatch = [
        cat for cat in all_categories_pydantic 
        if cat.source_category_id in categories_to_dispatch_ids
    ]

    # Only process the first category for local testing
    if categories_to_dispatch:
        # categories_to_process_single = [categories_to_dispatch[0]]
        
        for job_category in categories_to_dispatch:
            logger.info(
                "Dispatching crawl_and_store_category_urls task for local testing.",
                job_category_code=job_category.source_category_id,
                url_limit=url_limit,
            )
            crawl_and_store_category_urls(job_category.model_dump(), url_limit=url_limit)
    else:
        logger.info("No categories found to dispatch for testing.")