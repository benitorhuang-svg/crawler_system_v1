import os
import structlog
from collections import deque
from bs4 import BeautifulSoup

# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---

from crawler.worker import app
from crawler.database.schemas import SourcePlatform, UrlCategoryPydantic, CategorySourcePydantic
from crawler.database.repository import upsert_urls, upsert_url_categories, get_all_categories_for_platform
from crawler.project_cakeresume.client_cakeresume import fetch_cakeresume_job_urls
from crawler.config import (
    URL_CRAWLER_UPLOAD_BATCH_SIZE,
)
from crawler.project_cakeresume.config_cakeresume import (
    URL_CRAWLER_ORDER_BY_CAKERESUME,
    JOB_DETAIL_BASE_URL_CAKERESUME,
)

logger = structlog.get_logger(__name__)


@app.task
def crawl_and_store_cakeresume_category_urls(job_category: dict, url_limit: int = 0) -> None:
    """
    Celery task: Iterates through all pages of a specified CakeResume job category, fetches job URLs,
    and stores them in the database.
    """
    job_category = CategorySourcePydantic.model_validate(job_category)
    job_category_code = job_category.source_category_id
    global_job_url_set = set()
    current_batch_urls = []
    current_batch_url_categories = []
    recent_counts = deque(maxlen=4)

    current_page = 0
    max_page = 1

    logger.info(
        "Task started: crawling CakeResume job category URLs.", job_category_code=job_category_code, url_limit=url_limit
    )

    while True:
        if url_limit > 0 and len(global_job_url_set) >= url_limit:
            logger.info("URL limit reached. Ending task early.", job_category_code=job_category_code, url_limit=url_limit, collected_urls=len(global_job_url_set))
            break

        if current_page % 5 == 0:
            logger.info(
                "Current page being processed.",
                page=current_page,
                job_category_code=job_category_code,
            )

        html_content = fetch_cakeresume_job_urls(
            KEYWORDS="",
            CATEGORY=job_category_code,
            ORDER=URL_CRAWLER_ORDER_BY_CAKERESUME,
            PAGE_NUM=current_page,
        )

        if html_content is None:
            logger.error(
                "Failed to retrieve job URLs from CakeResume.",
                page=current_page,
                job_category_code=job_category_code,
            )
            break

        soup = BeautifulSoup(html_content, 'html.parser')
        job_urls_on_page = soup.find_all('a', class_='JobSearchItem_jobTitle__bu6yO')

        if not job_urls_on_page:
            logger.info("No more job URLs found for this category and page.", page=current_page, job_category_code=job_category_code)
            break

        for job_url_item in job_urls_on_page:
            job_link_suffix = job_url_item.get('href')
            if job_link_suffix:
                full_job_link = f"{JOB_DETAIL_BASE_URL_CAKERESUME}{job_link_suffix}"
                if full_job_link not in global_job_url_set:
                    global_job_url_set.add(full_job_link)
                    current_batch_urls.append(full_job_link)
                current_batch_url_categories.append(
                    UrlCategoryPydantic(
                        source_url=full_job_link,
                        source_category_id=job_category_code,
                    ).model_dump()
                )

        pagination_items = soup.find_all('a', class_='Pagination_itemNumber___enNq')
        if pagination_items:
            try:
                max_page = int(pagination_items[-1].text)
            except ValueError:
                logger.warning("Could not parse max_page from pagination items.", job_category_code=job_category_code)

        if len(current_batch_urls) >= URL_CRAWLER_UPLOAD_BATCH_SIZE:
            logger.info(
                "Batch upload size reached. Starting URL and URL-Category upload.",
                count=len(current_batch_urls),
                job_category_code=job_category_code,
            )
            upsert_urls(SourcePlatform.PLATFORM_CAKERESUME, current_batch_urls)
            upsert_url_categories(current_batch_url_categories)
            current_batch_urls.clear()
            current_batch_url_categories.clear()

        total_jobs = len(global_job_url_set)
        recent_counts.append(total_jobs)
        if len(recent_counts) == recent_counts.maxlen and len(set(recent_counts)) == 1:
            logger.info(
                "No new data found consecutively. Ending task early.",
                max_len=recent_counts.maxlen,
                job_category_code=job_category_code,
            )
            break

        current_page += 1
        if current_page > max_page:
            logger.info("Reached max page. Ending task.", current_page=current_page, max_page=max_page, job_category_code=job_category_code)
            break

    if current_batch_urls:
        logger.info(
            "Task completed. Storing remaining raw job URLs to database.",
            count=len(current_batch_urls),
            job_category_code=job_category_code,
        )
        upsert_urls(SourcePlatform.PLATFORM_CAKERESUME, current_batch_urls)
        upsert_url_categories(current_batch_url_categories)
    else:
        logger.info(
            "Task completed. No URLs collected, skipping database storage.",
            job_category_code=job_category_code,
        )

    logger.info("Task execution finished.", job_category_code=job_category_code)


if __name__ == "__main__":
    # To run this script for local testing, execute:
    # python -m crawler.project_cakeresume.task_urls_cakeresume
    # This will automatically use the 'test_db' as configured at the top of the script.

    from crawler.database.connection import initialize_database
    initialize_database()

    job_category_lists = get_all_categories_for_platform(SourcePlatform.PLATFORM_CAKERESUME)

    for job_category in job_category_lists:
        logger.info("Dispatching crawl_and_store_cakeresume_category_urls task for local testing.", job_category_code=job_category.source_category_id)
        crawl_and_store_cakeresume_category_urls(job_category.model_dump())