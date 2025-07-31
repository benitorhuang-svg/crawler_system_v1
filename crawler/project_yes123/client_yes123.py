import random
import time
from typing import Any, Dict, Optional

import requests
import structlog
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import urllib.parse

from crawler.config import (
    URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
    URL_CRAWLER_SLEEP_MAX_SECONDS,
    URL_CRAWLER_SLEEP_MIN_SECONDS,
)
from crawler.logging_config import configure_logging
from crawler.project_yes123.config_yes123 import (
    HEADERS_YES123,
    JOB_LISTING_BASE_URL_YES123,
    JOB_CAT_URL_YES123,
)

# Suppress only the single InsecureRequestWarning from urllib3 needed
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


configure_logging()
logger = structlog.get_logger(__name__)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True,
)
def _make_web_request(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
    verify: bool = True,
    log_context: Optional[Dict[str, Any]] = None,
) -> Optional[str]: # Return HTML content as string
    """
    通用的網頁請求函式，處理隨機延遲、請求發送、和錯誤處理。
    """
    if log_context is None:
        log_context = {}

    # Add random delay before making API request
    sleep_time = random.uniform(
        URL_CRAWLER_SLEEP_MIN_SECONDS, URL_CRAWLER_SLEEP_MAX_SECONDS
    )
    logger.debug("Sleeping before web request.", duration=sleep_time, **log_context)
    time.sleep(sleep_time)

    try:
        response = requests.request(
            method,
            url,
            headers=headers,
            params=params,
            timeout=timeout,
            verify=verify,
        )
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(
            "Network error during web request.",
            url=url,
            error=e,
            exc_info=True,
            **log_context,
        )
        raise  # Re-raise the exception to trigger tenacity retry
    except Exception as e:
        logger.error(
            "Unexpected error during web request.",
            url=url,
            error=e,
            exc_info=True,
            **log_context,
        )
        return None

def fetch_yes123_category_data(
    url: str = JOB_CAT_URL_YES123, headers: Dict[str, str] = HEADERS_YES123
) -> Optional[str]:
    """
    從 yes123 獲取職務分類的原始數據 (HTML 內容)。
    """
    return _make_web_request("GET", url, headers=headers, log_context={"api_type": "yes123_category_data"})

def yes123_url(
    KEYWORDS: str = "",
    CATEGORY: str = "",
    ORDER: str = "new",
    PAGE_NUM: int = 1,
) -> str:
    """
    這個函數會根據給定的關鍵字、類別、排序和頁碼參數，
    構建一個 yes123 求職網的完整職缺網址。

    參數:
    KEYWORDS (str): 職缺的關鍵字。
    CATEGORY (str): 職缺的類別代碼。
    ORDER (str, optional): 排序方式。預設為 "new" (最新)。
    PAGE_NUM (int, optional): 指定的頁碼。預設為 1。

    返回:
    str: 生成的 yes123 求職網址。
    """
    base_url = JOB_LISTING_BASE_URL_YES123
    params = {
        "p": PAGE_NUM,
        "s_key": KEYWORDS,
        "job_kind": CATEGORY,
        "order": ORDER,
    }
    query_string = urllib.parse.urlencode(params)
    return f"{base_url}?{query_string}"

def fetch_yes123_job_urls(
    KEYWORDS: str = "",
    CATEGORY: str = "",
    ORDER: str = "new",
    PAGE_NUM: int = 1,
) -> Optional[str]: # Returns HTML content of the job listing page
    """
    從 yes123 獲取職缺 URL 列表的原始數據 (HTML 內容)。
    """
    url = yes123_url(KEYWORDS, CATEGORY, ORDER, PAGE_NUM)
    return _make_web_request(
        "GET",
        url,
        headers=HEADERS_YES123,
        timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
        verify=False,
        log_context={
            "api_type": "yes123_job_urls",
            "keywords": KEYWORDS,
            "category": CATEGORY,
            "page": PAGE_NUM,
        },
    )

def fetch_yes123_job_data(job_url: str) -> Optional[str]: # Returns HTML content of the job detail page
    """
    從 yes123 職缺頁面抓取單一 URL 的資料 (HTML 內容)。
    """
    return _make_web_request(
        "GET",
        job_url,
        headers=HEADERS_YES123,
        timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
        verify=False,
        log_context={
            "api_type": "yes123_job_detail",
            "url": job_url,
        },
    )
