# import os
# # python -m crawler.project_1111.task_urls_1111
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---


import structlog

from typing import Set, List, Optional, Dict
import concurrent.futures

from crawler.worker import app
from crawler.database.connection import initialize_database
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
    get_all_crawled_category_ids_pandas,
    get_stale_crawled_category_ids_pandas,
)
from crawler.project_1111.client_1111 import fetch_job_urls_from_1111_api
from crawler.project_1111.parser_apidata_1111 import parse_job_list_json_to_pydantic
from crawler.config import (
    URL_CRAWLER_UPLOAD_BATCH_SIZE,
)
from crawler.project_1111.config_1111 import (
    URL_CRAWLER_ORDER_BY_1111,
)

# Define concurrency level for fetching pages
CONCURRENCY_LEVEL = 100 # Can be moved to config.py if needed

logger = structlog.get_logger(__name__)


def _fetch_and_parse_single_page(job_category_code: str, page_num: int) -> Optional[List[Dict]]:
    """
    Fetches job items for a single page from the 1111 API and parses them.
    Returns a list of job item dictionaries or None if an error occurs or no items are found.
    """
    api_response = fetch_job_urls_from_1111_api(
        KEYWORDS="",
        CATEGORY=job_category_code,
        ORDER=URL_CRAWLER_ORDER_BY_1111,
        PAGE_NUM=page_num,
    )

    if api_response is None:
        logger.error(
            "Failed to retrieve data from 1111 API for single page.",
            page=page_num,
            job_category_code=job_category_code,
        )
        return None

    job_items = api_response.get("result", {}).get("hits", [])
    if not isinstance(job_items, list):
        logger.error(
            "API response 'result.hits' format is incorrect or missing for single page.",
            page=page_num,
            job_category_code=job_category_code,
            api_data_type=type(job_items),
        )
        return None

    if not job_items:
        logger.debug(
            "No job items found on single page.",
            page=page_num,
            job_category_code=job_category_code,
        )
        return None
    
    return job_items


@app.task
def crawl_and_store_1111_category_urls(job_category: dict, url_limit: int = 0) -> int:
    """
    Celery task: Iterates through all pages of a specified 1111 job category, fetches job URLs
    and preliminary data, and stores them in the database in batches using concurrent fetching.
    """
    job_category = CategorySourcePydantic.model_validate(job_category)
    job_category_code = job_category.source_category_id
    
    global_job_url_set = set()
    current_batch_jobs = []
    current_batch_urls = []
    current_batch_url_categories = []

    logger.info(
        "Task started: crawling 1111 job category URLs and data.",
        job_category_code=job_category_code,
        url_limit=url_limit,
    )

    # Step 1: Fetch the first page synchronously to get totalPage
    first_page_job_items = _fetch_and_parse_single_page(job_category_code, 1)
    
    if first_page_job_items is None:
        logger.error(
            "Failed to fetch first page, cannot proceed with crawling.",
            job_category_code=job_category_code,
        )
        return 0

    # Get totalPage from the first page's API response
    # Re-fetch API response for page 1 to get pagination data
    api_response_first_page = fetch_job_urls_from_1111_api(
        KEYWORDS="",
        CATEGORY=job_category_code,
        ORDER=URL_CRAWLER_ORDER_BY_1111,
        PAGE_NUM=1,
    )
    total_pages = 1 # Default to 1 page if totalPage not found
    if api_response_first_page:
        pagination_data = api_response_first_page.get("result", {}).get("pagination", {})
        if "totalPage" in pagination_data:
            total_pages = pagination_data["totalPage"]
            logger.info(
                "Total pages discovered from API.",
                job_category_code=job_category_code,
                total_pages=total_pages,
            )
    
    # Process first page items
    for job_item in first_page_job_items:
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

    # Step 2: Use ThreadPoolExecutor for concurrent fetching of remaining pages
    pages_to_fetch = range(2, total_pages + 1) # Start from page 2
    if url_limit > 0:
        # Estimate how many pages are needed to reach url_limit
        # Assuming each page has URL_CRAWLER_UPLOAD_BATCH_SIZE items (approx)
        estimated_pages_for_limit = (url_limit - len(global_job_url_set)) // URL_CRAWLER_UPLOAD_BATCH_SIZE + 2
        pages_to_fetch = range(2, min(total_pages + 1, estimated_pages_for_limit))
        logger.info(
            "Adjusting pages to fetch based on URL limit.",
            job_category_code=job_category_code,
            url_limit=url_limit,
            estimated_pages=len(pages_to_fetch),
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY_LEVEL) as executor:
        future_to_page = {
            executor.submit(_fetch_and_parse_single_page, job_category_code, page_num): page_num
            for page_num in pages_to_fetch
            if (url_limit == 0 or len(global_job_url_set) < url_limit) # Only submit if limit not reached
        }

        for future in concurrent.futures.as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                job_items_on_page = future.result()
                if job_items_on_page:
                    logger.debug(
                        "Successfully fetched and parsed page.",
                        page=page_num,
                        job_category_code=job_category_code,
                        items_count=len(job_items_on_page),
                    )
                    for job_item in job_items_on_page:
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
                
                # Check if batch size reached after processing each page's items
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
                
                # Check URL limit again after processing a page
                if url_limit > 0 and len(global_job_url_set) >= url_limit:
                    logger.info(
                        "URL limit reached during concurrent fetching. Stopping further submissions.",
                        job_category_code=job_category_code,
                        url_limit=url_limit,
                        collected_urls=len(global_job_url_set),
                    )
                    # Cancel remaining futures if limit is reached
                    for remaining_future in future_to_page:
                        if not remaining_future.done():
                            remaining_future.cancel()
                    break # Break from the as_completed loop

            except concurrent.futures.CancelledError:
                logger.info("Future was cancelled due to URL limit being reached.", page=page_num)
            except Exception as exc:
                logger.error(
                    "Page generation failed.",
                    page=page_num,
                    job_category_code=job_category_code,
                    error=exc,
                    exc_info=True,
                )

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

    logger.info("Task execution finished.", job_category_code=job_category_code, total_collected=len(global_job_url_set))
    return len(global_job_url_set)


if __name__ == "__main__":
    initialize_database()

    n_days = 7  # Define n_days for local testing
    url_limit = 100000

    all_categories_pydantic: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_1111)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories_pydantic}
    all_crawled_category_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_1111)
    stale_crawled_category_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_1111, n_days)
    categories_to_dispatch_ids = (all_category_ids - all_crawled_category_ids) | stale_crawled_category_ids
    categories_to_dispatch = [
        cat for cat in all_categories_pydantic 
        if cat.source_category_id in categories_to_dispatch_ids
    ]


    if categories_to_dispatch:
        # categories_to_process_single = [categories_to_dispatch[0]]

        for job_category in categories_to_dispatch:
            logger.info(
                "Dispatching crawl_and_store_1111_category_urls task for local testing.",
                job_category_code=job_category.source_category_id,
                url_limit=url_limit,
            )
            crawl_and_store_1111_category_urls(job_category.model_dump(), url_limit=url_limit)
    else:
        logger.info("No categories found to dispatch for testing.")
