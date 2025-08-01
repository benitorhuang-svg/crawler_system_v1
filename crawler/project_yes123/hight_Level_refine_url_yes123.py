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
        "開始處理頁面",
        page_num=page_num,
        category=job_category_code,
        empty_count=consecutive_empty_count,
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
            logger.info("頁面找到 URLs", count=len(job_links), page=page_num, category=job_category_code)
            url_category_data = [
                UrlCategoryPydantic(source_url=link, source_category_id=job_category_code).model_dump()
                for link in job_links
            ]
            upsert_urls(SourcePlatform.PLATFORM_YES123, job_links)
            upsert_url_categories(url_category_data)
            next_empty_count = 0
        else:
            # 未找到 URL，增加計數器
            logger.warning("頁面為空或未找到 URLs", page=page_num, category=job_category_code)
            next_empty_count = consecutive_empty_count + 1

        # 決定是否繼續鏈接
        if next_empty_count >= CONSECUTIVE_EMPTY_PAGE_LIMIT:
            logger.info(
                "連續空頁面已達上限，終止任務鏈",
                limit=CONSECUTIVE_EMPTY_PAGE_LIMIT,
                category=job_category_code,
            )
            return

        # 派送下一個頁面的任務
        next_page_num = page_num + 1
        logger.info("鏈接下一頁任務", next_page=next_page_num, category=job_category_code)
        task_crawl_yes123_page_and_chain.delay(
            job_category_code=job_category_code,
            page_num=next_page_num,
            consecutive_empty_count=next_empty_count
        )

    except Exception as e:
        logger.error("處理頁面時發生未知嚴重錯誤，任務將重試", url=page_url, error=str(e))
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
        logger.error("無效的職缺類別資料，啟動失敗", data=job_category, error=str(e))
        return
    
    logger.info("啟動任務鏈", job_category_code=job_category_code)
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
    N_DAYS = 1
    
    logger.info("本地測試開始：獲取需要處理的職缺類別...")
    # (這部分的邏輯與之前相同)
    all_categories: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_YES123)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories}
    crawled_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YES123)
    stale_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YES123, N_DAYS)
    dispatch_ids = (all_category_ids - crawled_ids) | stale_ids
    categories_to_dispatch = [cat for cat in all_categories if cat.source_category_id in dispatch_ids]

    if not categories_to_dispatch:
        logger.info("沒有需要處理的職缺類別。")
        return

    logger.info(f"共發現 {len(categories_to_dispatch)} 個類別需要啟動爬取鏈。")
    
    # categories_to_dispatch = categories_to_dispatch[:1] 

    for category in categories_to_dispatch:
        logger.info("本地調度啟動任務", job_category_code=category.source_category_id)
        # 在本地直接調用啟動函式來模擬行為
        task_start_yes123_crawl_chain(category.model_dump())

if __name__ == "__main__":
    _run_local_test()