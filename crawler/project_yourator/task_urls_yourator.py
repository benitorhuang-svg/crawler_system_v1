import os
from typing import List, Set
from crawler.database.connection import initialize_database
from crawler.database.repository import (
    get_all_categories_for_platform,
    get_all_crawled_category_ids_pandas,
    get_stale_crawled_category_ids_pandas,
)
# # python -m crawler.project_yourator.task_urls_yourator
# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---


import structlog

from crawler.database.repository import (
    upsert_urls,
    upsert_url_categories,
    upsert_jobs,
)
from crawler.database.schemas import (
    SourcePlatform,
    UrlCategoryPydantic,
    CategorySourcePydantic,
)
from crawler.project_yourator.client_yourator import fetch_job_urls_from_yourator_api
from crawler.project_yourator.parser_apidata_yourator import parse_job_list_to_pydantic
from crawler.worker import app

logger = structlog.get_logger(__name__)


@app.task
def crawl_and_store_yourator_category_urls(job_category: dict, url_limit: int = 0):
    job_category = CategorySourcePydantic.model_validate(job_category)
    job_category_code = job_category.source_category_id

    page = 1
    total_urls = 0

    while True:
        if url_limit > 0 and total_urls >= url_limit:
            logger.info(
                "URL limit reached for category.",
                category_id=job_category_code,
                limit=url_limit,
                total_urls=total_urls,
            )
            break

        api_response = fetch_job_urls_from_yourator_api(
            page=page, category=job_category_code
        )

        if not api_response or "payload" not in api_response:
            logger.warning(
                "Invalid or empty API response.",
                page=page,
                category=job_category_code,
                response=api_response,
            )
            break

        payload = api_response["payload"]
        jobs = payload.get("jobs", [])

        if not jobs:
            logger.info(
                "No more jobs found for category on this page.",
                page=page,
                category=job_category_code,
            )
            break

        urls_to_add = []
        url_categories_to_add = []
        jobs_to_add = []

        for job_item in jobs:
            job_pydantic = parse_job_list_to_pydantic(job_item)
            if job_pydantic and job_pydantic.url:
                urls_to_add.append(job_pydantic.url)
                url_categories_to_add.append(
                    UrlCategoryPydantic(
                        source_url=job_pydantic.url,
                        source_category_id=job_category_code,
                    ).model_dump()
                )
                jobs_to_add.append(job_pydantic)

        if urls_to_add:
            upsert_urls(SourcePlatform.PLATFORM_YOURATOR, urls_to_add)
            upsert_url_categories(url_categories_to_add)
            upsert_jobs(jobs_to_add)
            total_urls += len(urls_to_add)
            logger.info(
                "Upserted URLs for category.",
                count=len(urls_to_add),
                page=page,
                category=job_category_code,
            )

        if not payload.get("hasMore", False):
            logger.info(
                "API indicated no more pages for category.",
                category_id=job_category_code,
                page=page,
            )
            break

        page += 1



if __name__ == "__main__":
    initialize_database()

    n_days = 7  # Define n_days for local testing
    url_limit = 100000

    all_categories_pydantic: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_YOURATOR)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories_pydantic}
    all_crawled_category_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YOURATOR)
    stale_crawled_category_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YOURATOR, n_days)
    categories_to_dispatch_ids = (all_category_ids - all_crawled_category_ids) | stale_crawled_category_ids
    categories_to_dispatch = [
        cat for cat in all_categories_pydantic 
        if cat.source_category_id in categories_to_dispatch_ids
    ]


    if categories_to_dispatch:
        # categories_to_process_single = [categories_to_dispatch[0]]

        for job_category in categories_to_dispatch:
            logger.info(
                "Dispatching crawl_and_store_yourator_category_urls task for local testing.",
                job_category_code=job_category.source_category_id,
                url_limit=url_limit,
            )
            crawl_and_store_yourator_category_urls(job_category.model_dump(), url_limit=url_limit)
    else:
        logger.info("No categories found to dispatch for testing.")
