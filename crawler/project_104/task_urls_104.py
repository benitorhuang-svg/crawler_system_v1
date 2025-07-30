import structlog
from collections import deque

from crawler.worker import app
from crawler.database.models import SourcePlatform, UrlCategoryPydantic
from crawler.database.repository import upsert_urls, upsert_url_categories
from crawler.api_clients.client_104 import fetch_job_urls_from_104_api

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
def crawl_and_store_category_urls(job_category_code: str, url_limit: int = 0) -> None:
    """
    Celery 任務：遍歷指定職缺類別的所有頁面，抓取職缺網址，並將其儲存到資料庫。

    :param job_category_code: 職缺類別代碼。
    :param url_limit: 限制抓取的 URL 數量。0 表示無限制。
    """
    global_job_url_set = set()
    current_batch_urls = []
    current_batch_url_categories = [] # 新增：用於儲存 URL-Category 關聯
    recent_counts = deque(maxlen=4)

    current_page = 1
    logger.info(
        "Task started: crawling job category URLs.", job_category_code=job_category_code, url_limit=url_limit
    )

    while True:
        # 檢查是否達到 URL 限制
        if url_limit > 0 and len(global_job_url_set) >= url_limit:
            logger.info("URL limit reached. Ending task early.", job_category_code=job_category_code, url_limit=url_limit, collected_urls=len(global_job_url_set))
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

        api_data = fetch_job_urls_from_104_api(
            URL_CRAWLER_BASE_URL_104,
            HEADERS_104_URL_CRAWLER,
            params,
            URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
            verify=False,
        )

        if api_data is None:
            logger.error(
                "Failed to retrieve job URLs from 104 API (error logged by client).",
                page=current_page,
                job_category_code=job_category_code,
            )
            break

        api_job_urls = api_data.get("data")
        if not isinstance(api_job_urls, list):
            logger.error(
                "API response 'data' format is incorrect or missing.",
                page=current_page,
                job_category_code=job_category_code,
                api_data_type=type(api_job_urls),
                api_data_sample=str(api_job_urls)[:100],
            )
            break

        for job_url_item in api_job_urls:
            job_link = job_url_item.get("link", {}).get("job")
            if job_link:
                if job_link not in global_job_url_set:
                    global_job_url_set.add(job_link)
                    current_batch_urls.append(job_link)
                # 無論是否是新的 URL，都將其與當前類別關聯
                current_batch_url_categories.append(
                    UrlCategoryPydantic(
                        source_url=job_link,
                        source_category_id=job_category_code,
                    ).model_dump()
                )

        if len(current_batch_urls) >= URL_CRAWLER_UPLOAD_BATCH_SIZE:
            logger.info(
                "Batch upload size reached. Starting URL and URL-Category upload.",
                count=len(current_batch_urls),
                job_category_code=job_category_code,
            )
            upsert_urls(SourcePlatform.PLATFORM_104, current_batch_urls)
            upsert_url_categories(current_batch_url_categories) # 上傳 URL-Category 關聯
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

    if current_batch_urls:
        logger.info(
            "Task completed. Storing remaining raw job URLs to database.",
            count=len(current_batch_urls),
            job_category_code=job_category_code,
        )
        upsert_urls(SourcePlatform.PLATFORM_104, current_batch_urls)
        upsert_url_categories(current_batch_url_categories) # 上傳剩餘的 URL-Category 關聯
    else:
        logger.info(
            "Task completed. No URLs collected, skipping database storage.",
            job_category_code=job_category_code,
        )

    logger.info("Task execution finished.", job_category_code=job_category_code)
