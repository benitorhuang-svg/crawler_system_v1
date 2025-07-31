import json
import random
import time
from typing import Any, Dict, Optional, Union, List

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
from crawler.project_1111.config_1111 import (
    HEADERS_1111_JOB_API,
    JOB_API_BASE_URL_1111,
    JOB_CAT_URL_1111,
    HEADERS_1111,
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
def _make_api_request(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
    verify: bool = False,
    log_context: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    通用的 API 請求函式，處理隨機延遲、請求發送、JSON 解析和錯誤處理。
    """
    if log_context is None:
        log_context = {}

    # Add random delay before making API request
    sleep_time = random.uniform(
        URL_CRAWLER_SLEEP_MIN_SECONDS, URL_CRAWLER_SLEEP_MAX_SECONDS
    )
    logger.debug("Sleeping before API request.", duration=sleep_time, **log_context)
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
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.error(
            "Network error during API request.",
            url=url,
            error=e,
            exc_info=True,
            **log_context,
        )
        raise  # Re-raise the exception to trigger tenacity retry
    except json.JSONDecodeError:
        logger.error(
            "Failed to parse JSON response from API.",
            url=url,
            exc_info=True,
            **log_context,
        )
        return None
    except Exception as e:
        logger.error(
            "Unexpected error during API request.",
            url=url,
            error=e,
            exc_info=True,
            **log_context,
        )
        return None

def fetch_category_data_from_1111_api(
    api_url: str = JOB_CAT_URL_1111, headers: Dict[str, str] = HEADERS_1111
) -> Optional[Dict[str, Any]]:
    """
    從 1111 API 獲取職務分類的原始數據。
    """
    return _make_api_request(
        "GET",
        api_url,
        headers=headers,
        log_context={"api_type": "1111_category_data"},
    )

def catch_1111_url(KEYWORDS: str, CATEGORY: Union[str, List[str]], ORDER: str = "date", PAGE_NUM: int = 1, USE_API: bool = True):
    """
    這個函數會根據給定的關鍵字、類別、排序和頁碼參數，
    構建一個 1111 求職網的完整職缺網址或 API 網址。

    參數:
    KEYWORDS (str): 職缺的關鍵字，例如 "雲端工程師"。若無則傳入空字串 ""。
    CATEGORY (str or list): 職缺的類別代碼，例如 "140100" 或者類別代碼的列表。
                            若無則傳入空字串 ""。
    ORDER (str, optional): 排序方式。可選值為 "relevance" (相關性) 或 "date" (最新日期)。
                           預設為 "date"。
    PAGE_NUM (int, optional): 指定的頁碼。預設為 1。
    USE_API (bool, optional): 是否使用 API 網址。預設為 False。

    返回:
    str: 生成的 1111 求職網址或 API 網址。
    """
    BASE_URL = "https://www.1111.com.tw/search/job"
    API_URL = JOB_API_BASE_URL_1111

    # 確保頁碼至少為 1，避免負數或 0 造成計算錯誤
    safe_page_num = max(1, PAGE_NUM)

    if USE_API:
        params = {
            "page": safe_page_num,
            "fromOffset": 0,
            "sortBy": "ab" if ORDER == "relevance" else "da",
            "sortOrder": "desc",
        }

        # 如果有提供職務類別，加入到參數中
        if CATEGORY:
            if isinstance(CATEGORY, list):
                params["d0"] = ",".join(CATEGORY)
            else:
                params["d0"] = CATEGORY
        
        if KEYWORDS and KEYWORDS != "":
            params["keyword"] = KEYWORDS

        query_string = urllib.parse.urlencode(params)
        return f"{API_URL}?{query_string}"
    else:
        params = {
            "page": safe_page_num,
            "col": "ab" if ORDER == "relevance" else "da",
            "sort": "desc",
            "ks": KEYWORDS,
            "d0": ",".join(CATEGORY) if isinstance(CATEGORY, list) else CATEGORY,
        }

        query_string = urllib.parse.urlencode(params)
        return f"{BASE_URL}?{query_string}"

def fetch_job_urls_from_1111_api(
    KEYWORDS: str,
    CATEGORY: Union[str, List[str]],
    ORDER: str = "date",
    PAGE_NUM: int = 1,
) -> Optional[Dict[str, Any]]:
    """
    從 1111 API 獲取職缺 URL 列表的原始數據。
    """
    api_url = catch_1111_url(KEYWORDS, CATEGORY, ORDER, PAGE_NUM, USE_API=True)
    return _make_api_request(
        "GET",
        api_url,
        headers=HEADERS_1111_JOB_API,
        timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
        verify=False,
        log_context={
            "api_type": "1111_job_urls",
            "keywords": KEYWORDS,
            "category": CATEGORY,
            "page": PAGE_NUM,
        },
    )

def fetch_job_data_from_1111_web(job_url: str) -> Optional[Dict[str, Any]]:
    """
    從 1111 職缺頁面抓取單一 URL 的資料。
    """
    try:
        response = requests.get(job_url, verify=False, timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return {"content": response.text} # Return content for BeautifulSoup parsing
    except requests.exceptions.RequestException as e:
        logger.error(
            "Network error during 1111 job detail request.",
            url=job_url,
            error=e,
            exc_info=True,
        )
        raise # Re-raise to trigger tenacity retry if applied to this function
    except Exception as e:
        logger.error(
            "Unexpected error during 1111 job detail request.",
            url=job_url,
            error=e,
            exc_info=True,
        )
        return None
