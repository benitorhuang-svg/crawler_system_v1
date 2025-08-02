# import os
# # python -m crawler.project_yes123.task_urls_yes123
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import structlog


import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import urllib3
from collections import deque
from typing import Set, List, Tuple
import ssl # Added for SSLContext

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
from crawler.config import URL_CRAWLER_UPLOAD_BATCH_SIZE
from crawler.project_yes123.config_yes123 import HEADERS_YES123

# Suppress only the single InsecureRequestWarning from urllib3 needed
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = structlog.get_logger(__name__)

# 常數定義
BASE_URL = "https://www.yes123.com.tw"
JOB_LIST_URL_TEMPLATE = f"{BASE_URL}/wk_index/joblist.asp?find_work_mode1={{job_category_code}}&order_by=m_date&order_ascend=desc"
JOB_LINK_SELECTOR = "a.Job_opening_block"
PAGE_SELECTOR = "#inputState option"
DEFAULT_TIMEOUT = 15
CONSECUTIVE_EMPTY_PAGE_LIMIT = 4


def create_session_with_retries() -> requests.Session:
    """
    建立一個具有重試機制的 requests.Session 物件。
    """
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)

    # Attempt to force TLSv1.2
    try:
        context = ssl.create_default_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        adapter.ssl_context = context
    except AttributeError:
        logger.warning("SSLContext.minimum_version not available, cannot force TLS version.")

    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS_YES123)
    # yes123 的 SSL 憑證有時會有問題，因此設定 verify=False
    session.verify = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return session


def fetch_yes123_job_urls_for_page(
    session: requests.Session, base_url: str, page: int = 1, timeout: int = 15
) -> Tuple[List[str], int]:
    """
    從 yes123 網站擷取指定頁面的職缺網址和最大頁數。

    Args:
        session (requests.Session): 用於發送請求的 Session 物件。
        base_url (str): 搜尋結果的第一頁網址 (不含分頁參數)。
        page (int): 欲抓取的頁碼。預設為 1。
        timeout (int): 請求的超時秒數。預設為 15。

    Returns:
        Tuple[List[str], int]: 一個包含職缺 URL 列表和最大頁數的元組。

    Raises:
        requests.exceptions.RequestException: 當請求失敗時拋出。
    """
    job_url_list = []
    max_total_pages = 1000
    current_url = f"{base_url}&strrec={(page - 1) * 30}"

    try:
        response = session.get(current_url, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8-sig' # Ensure correct encoding
        soup = BeautifulSoup(response.text, "html.parser")

        link_tags = soup.select(JOB_LINK_SELECTOR)
        for tag in link_tags:
            if "href" in tag.attrs:
                full_url = urljoin(current_url, tag["href"])
                job_url_list.append(full_url)

        options = soup.select(PAGE_SELECTOR)
        max_total_pages = max(
            (int(opt["value"]) for opt in options if opt.get("value", "").isdigit()),
            default=1
        )

    except requests.exceptions.RequestException as e:
        logger.error("請求 yes123 頁面時發生錯誤", url=current_url, error=str(e))
    except Exception as e:
        logger.error("解析 yes123 頁面時發生未知錯誤", url=current_url, error=str(e))

    return list(set(job_url_list)), max_total_pages # Return unique URLs for the current page and max_total_pages


@app.task
def crawl_and_store_yes123_category_urls(job_category: dict, url_limit: int = 0) -> None:
    _crawl_and_store_yes123_category_urls_core(job_category, url_limit)


def _crawl_and_store_yes123_category_urls_core(job_category: dict, url_limit: int = 0) -> None:
    """
    Core function: Iterates through all pages of a specified yes123 job category, fetches job URLs,
    and stores them in the database in batches.
    """
    try:
        category = CategorySourcePydantic.model_validate(job_category)
        job_category_code = category.source_category_id
    except Exception as e:
        logger.error("無效的職缺類別資料", data=job_category, error=str(e))
        return
 
    logger.info(
        "開始爬取 yes123 職缺類別的 URL",
        job_category_code=job_category_code,
        url_limit=url_limit or "無限制",
    )
    logger.debug("URL_CRAWLER_UPLOAD_BATCH_SIZE is set to:", batch_size=URL_CRAWLER_UPLOAD_BATCH_SIZE)

    global_job_url_set: Set[str] = set()
    current_batch_urls: List[str] = []
    current_batch_url_categories: List[dict] = []
    recent_url_counts = deque(maxlen=CONSECUTIVE_EMPTY_PAGE_LIMIT)
    max_pages = 100000
    page = 1

    session = create_session_with_retries()
    base_category_url = JOB_LIST_URL_TEMPLATE.format(job_category_code=job_category_code)

    while True:
        # 檢查是否達到 URL 數量限制
        if url_limit and len(global_job_url_set) >= url_limit:
            logger.info(
                "已達到 URL 數量限制，提前結束任務",
                job_category_code=job_category_code,
                collected_urls=len(global_job_url_set),
            )
            break

        # 檢查是否已超過最大頁數
        if page > max_pages:
            logger.info("已處理完所有頁面，任務即將完成", max_pages=max_pages, job_category_code=job_category_code)
            break

        if page % 5 == 1: # Log every 5 pages
            logger.info(
                "正在處理頁面",
                page=page,
                job_category_code=job_category_code,
            )

        # 擷取職缺 URL 和最大頁數
        page_job_links, discovered_max_pages = fetch_yes123_job_urls_for_page(session, base_category_url, page=page) 

        if page == 1: # Update max_pages only on the first page fetch
            max_pages = discovered_max_pages
            logger.info("檢測到總頁數", max_pages=max_pages, job_category_code=job_category_code)

        if not page_job_links:
            logger.warning("當前頁面未找到職缺連結", page=page, job_category_code=job_category_code)

        new_urls_on_page = 0
        for full_job_link in page_job_links:
            if full_job_link not in global_job_url_set:
                global_job_url_set.add(full_job_link)
                current_batch_urls.append(full_job_link)
                current_batch_url_categories.append(
                    UrlCategoryPydantic(
                        source_url=full_job_link,
                        source_category_id=job_category_code,
                    ).model_dump()
                )
                new_urls_on_page += 1

        logger.debug(
            "頁面處理完畢",
            page=page,
            new_urls=new_urls_on_page,
            total_urls=len(global_job_url_set),
        )

        # 檢查是否連續多頁沒有新 URL
        recent_url_counts.append(len(global_job_url_set))
        if len(recent_url_counts) == CONSECUTIVE_EMPTY_PAGE_LIMIT and len(set(recent_url_counts)) == 3:
            logger.info(
                "連續多頁未發現新的 URL，提前結束任務",
                job_category_code=job_category_code,
            )
            break
 
        # 如果批次大小達到上限，則上傳資料
        if len(current_batch_urls) >= URL_CRAWLER_UPLOAD_BATCH_SIZE:
            logger.info(
                "達到批次上傳大小，開始上傳 URL",
                count=len(current_batch_urls),
                job_category_code=job_category_code,
            )
            upsert_urls(SourcePlatform.PLATFORM_YES123, current_batch_urls)
            upsert_url_categories(current_batch_url_categories)
            current_batch_urls.clear()
            current_batch_url_categories.clear()
            logger.info("批次上傳完成")
 
        page += 1

    # 上傳剩餘的 URL
    if current_batch_urls:
        logger.info(
            "任務完成，正在儲存剩餘的 URL 到資料庫",
            count=len(current_batch_urls),
            job_category_code=job_category_code,
        )
        upsert_urls(SourcePlatform.PLATFORM_YES123, current_batch_urls)
        upsert_url_categories(current_batch_url_categories)
    
    logger.info("任務執行完畢", job_category_code=job_category_code, total_collected=len(global_job_url_set))





if __name__ == "__main__":
    initialize_database()


    n_days = 7  # Define n_days for local testing
    url_limit = 1000000 # Set a high limit for full crawling during local testing

    all_categories_pydantic: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_YES123)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories_pydantic}
    all_crawled_category_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YES123)
    stale_crawled_category_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YES123, n_days)
    categories_to_dispatch_ids = (all_category_ids - all_crawled_category_ids) | stale_crawled_category_ids
    categories_to_dispatch = [
        cat for cat in all_categories_pydantic 
        if cat.source_category_id in categories_to_dispatch_ids
    ]

    # Only process the first category for local testing
    if categories_to_dispatch:
        # categories_to_process_single = [categories_to_dispatch[0]] # Uncomment to process only the first category
        
        for job_category in categories_to_dispatch:
            logger.info(
                "Dispatching crawl_and_store_yes123_category_urls task for local testing.",
                job_category_code=job_category.source_category_id,
                url_limit=url_limit,
            )
            crawl_and_store_yes123_category_urls(job_category.model_dump(), url_limit=url_limit)
    else:
        logger.info("No categories found to dispatch for testing.")
