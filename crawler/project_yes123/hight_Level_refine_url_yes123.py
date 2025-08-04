import os
import structlog
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import urllib3
from typing import Set, List

# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
    # os.environ.setdefault('CRAWLER_DB_NAME', 'test_db')
# --- End Local Test Environment Setup ---

from crawler.worker import app
from crawler.database.schemas import (
    SourcePlatform,
    UrlCategoryPydantic,
    CategorySourcePydantic,
)
from crawler.database.repository import (
    upsert_urls,
    upsert_url_categories,
    get_all_categories_for_platform,
    get_all_crawled_category_ids_pandas,
    get_stale_crawled_category_ids_pandas,
)
from crawler.database.connection import initialize_database
from crawler.project_yes123.config_yes123 import HEADERS_YES123

# --- 常數定義 ---
BASE_URL = "https://www.yes123.com.tw"
JOB_LIST_URL_TEMPLATE = f"{BASE_URL}/wk_index/joblist.asp?find_work_mode1={{job_category_code}}&order_by=m_date&order_ascend=desc"
JOB_LINK_SELECTOR = "a.Job_opening_block"
DEFAULT_TIMEOUT = 15
CONSECUTIVE_EMPTY_PAGE_LIMIT = 3  # 連續 3 個空頁面後就停止

logger = structlog.get_logger(__name__)

# --- 網路請求與共享 Session ---
def create_session_with_retries() -> requests.Session:
    # (此函式與之前版本相同，保持不變)
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS_YES123)
    session.verify = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return session

# --- Celery 任務定義 ---

@app.task(
    bind=True, 
    autoretry_for=(requests.exceptions.RequestException,), 
    retry_kwargs={'max_retries': 3, 'countdown': 5}
)
def task_crawl_yes123_page_and_chain(self, job_category_code: str, page_num: int, consecutive_empty_count: int):
    """
    【處理與鏈接任務】
    爬取單一頁面，如果找到內容則繼續鏈接下一頁任務；
    如果頁面為空，則增加空頁計數，直到達到上限為止。
    """
    page_url = f"{JOB_LIST_URL_TEMPLATE.format(job_category_code=job_category_code)}&strrec={(page_num - 1) * 30}"
    logger.info(
        "Starting to process page.",
        event="start_processing_page",
        page_num=page_num,
        category=job_category_code,
        empty_count=consecutive_empty_count,
        platform=SourcePlatform.PLATFORM_YES123,
        component="high_level_crawler",
    )
    
    session = create_session_with_retries()
    
    try:
        response = session.get(page_url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        response.encoding = 'utf-8-sig'
        soup = BeautifulSoup(response.text, "html.parser")

        job_links = [
            urljoin(BASE_URL, tag["href"])
            for tag in soup.select(JOB_LINK_SELECTOR)
            if "href" in tag.attrs
        ]

        next_empty_count = 0
        if job_links:
            # 找到 URL，存儲數據並重置計數器
            logger.info("Page found URLs.",
                event="page_found_urls",
                count=len(job_links),
                page=page_num,
                category=job_category_code,
                platform=SourcePlatform.PLATFORM_YES123,
                component="high_level_crawler",
            )
            url_category_data = [
                UrlCategoryPydantic(source_url=link, source_category_id=job_category_code).model_dump()
                for link in job_links
            ]
            upsert_urls(SourcePlatform.PLATFORM_YES123, job_links)
            upsert_url_categories(url_category_data)
            next_empty_count = 0
        else:
            # 未找到 URL，增加計數器
            logger.warning("Page is empty or no URLs found.",
                event="empty_page_no_urls",
                page=page_num,
                category=job_category_code,
                platform=SourcePlatform.PLATFORM_YES123,
                component="high_level_crawler",
            )
            next_empty_count = consecutive_empty_count + 1

        # 決定是否繼續鏈接
        if next_empty_count >= CONSECUTIVE_EMPTY_PAGE_LIMIT:
            logger.info("Consecutive empty pages limit reached, terminating task chain.",
                event="consecutive_empty_pages_limit_reached",
                limit=CONSECUTIVE_EMPTY_PAGE_LIMIT,
                category=job_category_code,
                platform=SourcePlatform.PLATFORM_YES123,
                component="high_level_crawler",
            )
            return

        # 派送下一個頁面的任務
        next_page_num = page_num + 1
        logger.info("Chaining next page task.",
            event="chaining_next_page_task",
            next_page=next_page_num,
            category=job_category_code,
            platform=SourcePlatform.PLATFORM_YES123,
            component="high_level_crawler",
        )
        task_crawl_yes123_page_and_chain.delay(
            job_category_code=job_category_code,
            page_num=next_page_num,
            consecutive_empty_count=next_empty_count
        )

    except Exception as e:
        logger.error(
            "Network error during web request.",
            event="network_error_web_request",
            url=page_url,
            error=str(e),
            platform=SourcePlatform.PLATFORM_YES123,
            component="high_level_crawler",
            exc_info=True,
        )
        # 對於非請求錯誤，也觸發重試
        raise self.retry(exc=e)


@app.task(name="tasks.start_yes123_crawl")
def task_start_yes123_crawl_chain(job_category: dict):
    """
    【啟動任務】
    這是爬取一個完整職缺類別的入口點。
    它只負責啟動第一個頁面的爬取任務。
    """
    try:
        category = CategorySourcePydantic.model_validate(job_category)
        job_category_code = category.source_category_id
    except Exception as e:
        logger.error("Invalid job category data, failed to start.",
            event="invalid_job_category_data",
            data=job_category,
            error=str(e),
            platform=SourcePlatform.PLATFORM_YES123,
            component="high_level_crawler",
            exc_info=True,
        )
        return
    
    logger.info("Starting task chain.",
        event="start_task_chain",
        job_category_code=job_category_code,
        platform=SourcePlatform.PLATFORM_YES123,
        component="high_level_crawler",
    )
    # 啟動鏈條的第一環：從第 1 頁開始，空頁計數為 0
    task_crawl_yes123_page_and_chain.delay(
        job_category_code=job_category_code,
        page_num=1,
        consecutive_empty_count=0
    )


# --- 本地測試執行區塊 ---
def _run_local_test():
    """執行本地測試的函式。"""
    initialize_database()
    n_days = 7
    
    logger.info("Starting local test: fetching job categories to process...",
        event="start_local_test_fetching_categories",
        platform=SourcePlatform.PLATFORM_YES123,
        component="high_level_crawler",
    )
    # (這部分的邏輯與之前相同)
    all_categories: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_YES123)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories}
    crawled_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YES123)
    stale_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YES123, n_days)
    dispatch_ids = (all_category_ids - crawled_ids) | stale_ids
    categories_to_dispatch = [cat for cat in all_categories if cat.source_category_id in dispatch_ids]

    if not categories_to_dispatch:
        logger.info("No job categories to process.",
            event="no_job_categories_to_process",
            platform=SourcePlatform.PLATFORM_YES123,
            component="high_level_crawler",
        )
        return

    logger.info("Found categories to start crawling chain.",
        event="found_categories_to_crawl",
        count=len(categories_to_dispatch),
        platform=SourcePlatform.PLATFORM_YES123,
        component="high_level_crawler",
    )
    
    # categories_to_dispatch = categories_to_dispatch[:1] 

    for category in categories_to_dispatch:
        logger.info("Local dispatch starting task.",
            event="local_dispatch_start_task",
            job_category_code=category.source_category_id,
            platform=SourcePlatform.PLATFORM_YES123,
            component="high_level_crawler",
        )
        # 在本地直接調用啟動函式來模擬行為
        task_start_yes123_crawl_chain(category.model_dump())

if __name__ == "__main__":
    _run_local_test()