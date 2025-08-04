import os
import ssl
import time
import random # Added import for random
from typing import List, Set
import structlog
from urllib.parse import urljoin

from crawler.database.connection import initialize_database
from crawler.database.repository import (
    upsert_urls,
    get_all_categories_for_platform,
    get_all_crawled_category_ids_pandas,
    get_stale_crawled_category_ids_pandas,
)
from crawler.database.schemas import (
    SourcePlatform,
    CategorySourcePydantic,
)
from crawler.project_yes123.client_yes123 import fetch_yes123_page
from crawler.project_yes123.config_yes123 import (
    JOB_LISTING_BASE_URL_YES123,
    URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
    URL_CRAWLER_SLEEP_MIN_SECONDS,
    URL_CRAWLER_SLEEP_MAX_SECONDS,
)
from crawler.project_yes123.hight_Level_refine_url_yes123 import (
    extract_job_links_from_yes123_html,
    get_max_page_yes123,
)
from crawler.worker import app
from crawler.config import get_db_name_for_platform

logger = structlog.get_logger(__name__)

# 全局集合用於儲存已收集的職缺 URL，避免重複
global_job_url_set: Set[str] = set()


@app.task
def crawl_and_store_yes123_category_urls(job_category: dict, url_limit: int = 0):
    job_category = CategorySourcePydantic.model_validate(job_category)
    job_category_code = job_category.source_category_id
    db_name = get_db_name_for_platform(SourcePlatform.PLATFORM_YES123.value)

    # 嘗試強制使用 TLS 1.2 或更高版本
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    except AttributeError:
        logger.warning(
            "SSLContext.minimum_version not available, cannot force TLS version.",
            platform=SourcePlatform.PLATFORM_YES123,
            component="task",
        )
        ssl_context = None

    page = 1
    max_pages = float('inf')  # 初始設定為無限大，直到從頁面獲取實際值
    collected_urls_count = 0

    while page <= max_pages:
        if url_limit > 0 and collected_urls_count >= url_limit:
            logger.info(
                "URL limit reached for category.",
                category_id=job_category_code,
                limit=url_limit,
                total_urls=collected_urls_count,
                platform=SourcePlatform.PLATFORM_YES123,
                component="task",
            )
            break

        current_url = f"{JOB_LISTING_BASE_URL_YES123}&w={job_category_code}&page={page}"
        logger.info(
            "Fetching page.",
            url=current_url,
            page=page,
            category_id=job_category_code,
            platform=SourcePlatform.PLATFORM_YES123,
            component="task",
        )

        try:
            html_content = fetch_yes123_page(
                current_url,
                timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
                ssl_context=ssl_context,
            )
            if html_content is None:
                logger.warning(
                    "Failed to fetch HTML content for page.",
                    url=current_url,
                    page=page,
                    category_id=job_category_code,
                    platform=SourcePlatform.PLATFORM_YES123,
                    component="task",
                )
                page += 1
                continue

            if page == 1:
                # 只有在第一頁時才獲取最大頁數
                max_pages_from_html = get_max_page_yes123(html_content)
                if max_pages_from_html is not None:
                    max_pages = max_pages_from_html
                    logger.info(
                        "Detected total pages.",
                        max_pages=max_pages,
                        category_id=job_category_code,
                        platform=SourcePlatform.PLATFORM_YES123,
                        component="task",
                    )
                else:
                    logger.warning(
                        "Could not determine max pages from HTML. Assuming single page.",
                        url=current_url,
                        platform=SourcePlatform.PLATFORM_YES123,
                        component="task",
                    )
                    max_pages = 1 # 如果無法獲取最大頁數，則只處理當前頁

            job_links = extract_job_links_from_yes123_html(html_content)
            if not job_links:
                logger.info(
                    "No job links found on page.",
                    page=page,
                    category_id=job_category_code,
                    platform=SourcePlatform.PLATFORM_YES123,
                    component="task",
                )
                # 如果當前頁面沒有職缺連結，但不是最後一頁，則繼續下一頁
                if page < max_pages:
                    page += 1
                    continue
                else:
                    break # 如果是最後一頁且沒有連結，則結束

            new_urls_on_page = []
            for link in job_links:
                full_url = urljoin(JOB_LISTING_BASE_URL_YES123, link)
                if full_url not in global_job_url_set:
                    global_job_url_set.add(full_url)
                    new_urls_on_page.append(full_url)

            if new_urls_on_page:
                upsert_urls(SourcePlatform.PLATFORM_YES123, new_urls_on_page, db_name=db_name)
                collected_urls_count += len(new_urls_on_page)
                logger.info(
                    "New URLs upserted.",
                    count=len(new_urls_on_page),
                    total_collected=collected_urls_count,
                    page=page,
                    category_id=job_category_code,
                    platform=SourcePlatform.PLATFORM_YES123,
                    component="task",
                )
            else:
                logger.info(
                    "No new unique URLs found on this page.",
                    page=page,
                    category_id=job_category_code,
                    platform=SourcePlatform.PLATFORM_YES123,
                    component="task",
                )

            page += 1
            time.sleep(random.uniform(URL_CRAWLER_SLEEP_MIN_SECONDS, URL_CRAWLER_SLEEP_MAX_SECONDS))

        except Exception as e:
            logger.error(
                "Error processing page.",
                url=current_url,
                page=page,
                category_id=job_category_code,
                error=str(e),
                platform=SourcePlatform.PLATFORM_YES123,
                component="task",
                exc_info=True,
            )
            page += 1 # 遇到錯誤也嘗試跳到下一頁，避免卡死
            continue

    logger.info(
        "Crawling task finished for job category.",
        job_category_code=job_category_code,
        total_collected=collected_urls_count,
        platform=SourcePlatform.PLATFORM_YES123,
        component="task",
    )


if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
    initialize_database()

    n_days = 7  # Define n_days for local testing
    url_limit = 100000

    all_categories_pydantic: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_YES123, db_name=None)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories_pydantic}
    all_crawled_category_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YES123, db_name=None)
    stale_crawled_category_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YES123, n_days, db_name=None)
    categories_to_dispatch_ids = (all_category_ids - all_crawled_category_ids) | stale_crawled_category_ids
    categories_to_dispatch = [
        cat for cat in all_categories_pydantic
        if cat.source_category_id in categories_to_dispatch_ids
    ]
    categories_to_dispatch.sort(key=lambda x: x.source_category_id)

    if categories_to_dispatch:
        logger.info(
            "Found categories to dispatch for local testing.",
            count=len(categories_to_dispatch),
            platform=SourcePlatform.PLATFORM_YES123,
            component="task",
        )
        for job_category in categories_to_dispatch:
            logger.info(
                "Dispatching crawl_and_store_yes123_category_urls task for local testing.",
                job_category_code=job_category.source_category_id,
                url_limit=url_limit,
                platform=SourcePlatform.PLATFORM_YES123,
                component="task",
            )
            crawl_and_store_yes123_category_urls(job_category.model_dump(), url_limit=url_limit)
    else:
        logger.info(
            "No categories found to dispatch for testing.",
            platform=SourcePlatform.PLATFORM_YES123,
            component="task",
        )