import json
import random
import time
from typing import Any, Dict, Optional

import requests
import structlog
from bs4 import BeautifulSoup
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from crawler.config import (
    URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
    URL_CRAWLER_SLEEP_MAX_SECONDS,
    URL_CRAWLER_SLEEP_MIN_SECONDS,
)
from crawler.logging_config import configure_logging
from crawler.project_cakeresume.config_cakeresume import (
    HEADERS_CAKERESUME,
    JOB_CAT_URL_CAKERESUME,
    JOB_LISTING_BASE_URL_CAKERESUME,
)

# Suppress only the single InsecureRequestWarning from urllib3 needed
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


configure_logging()
logger = structlog.get_logger(__name__)


def log_before_retry(retry_state: "RetryCallState") -> None:
    """Log before retrying a request, showing attempt number and wait time."""
    logger.warning(
        "Request failed, retrying...",
        attempt=retry_state.attempt_number,
        wait_seconds=retry_state.next_action.sleep,
        error=retry_state.outcome.exception(),
    )


@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=1, min=4, max=30),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before_sleep=log_before_retry,
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
) -> Optional[str]:  # Return HTML content as string
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
        # If it's a 404 error, it might mean the category has no jobs.
        # Log a warning and return None so the caller can handle it without retrying.
        if (
            isinstance(e, requests.exceptions.HTTPError)
            and e.response.status_code == 404
        ):
            logger.warning(
                "HTTP 404 Not Found for URL. This might indicate an empty category.",
                url=url,
                **log_context,
            )
            return None

        # For other network errors, log an error and re-raise to trigger tenacity retry.
        logger.error(
            "Network error during web request.",
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

def fetch_cakeresume_category_page_html(
    url: str = JOB_CAT_URL_CAKERESUME, headers: Dict[str, str] = HEADERS_CAKERESUME
) -> Optional[str]:
    """
    從 CakeResume 獲取職務分類頁面的原始 HTML 內容。
    """
    return _make_web_request("GET", url, headers=headers, log_context={"api_type": "cakeresume_category_page_html"})

def extract_next_data_json_from_html(html_content: str) -> Optional[Dict[str, Any]]:
    """
    從 HTML 內容中提取 __NEXT_DATA__ script 標籤的 JSON 內容。
    """
    if not html_content:
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    data_script = soup.find('script', id='__NEXT_DATA__')

    if data_script and data_script.string:
        try:
            return json.loads(data_script.string)
        except (json.JSONDecodeError, KeyError) as e:
            logger.error("Failed to parse JSON data from __NEXT_DATA__ script.", error=e, exc_info=True)
            return None
    return None

def fetch_cakeresume_category_data(
    url: str = JOB_CAT_URL_CAKERESUME, headers: Dict[str, str] = HEADERS_CAKERESUME
) -> Optional[Dict[str, Any]]:
    """
    從 CakeResume 獲取職務分類的原始數據 (主要為 sector 資料)。
    """
    html_content = fetch_cakeresume_category_page_html(url, headers)
    if html_content:
        data = extract_next_data_json_from_html(html_content)
        if data:
            # The relevant data is nested under props.pageProps._nextI18Next.initialI18nStore.zh-TW.sector
            i18n_store_zh_tw_sector = data.get('props', {}).get('pageProps', {}).get('_nextI18Next', {}).get('initialI18nStore', {}).get('zh-TW', {}).get('sector', {})
            return {'initialI18nStore': {'zh-TW': {'sector': i18n_store_zh_tw_sector}}}
    return None

def cake_me_url(KEYWORDS: str, CATEGORY: str, ORDER: Optional[str] = None) -> str:
    """
    這個函數會根據給定的關鍵字和類別參數構建一個完整的職缺網址。
    如果同時提供了關鍵字和類別，將會包含兩者；如果只提供其中一個，則只會包含該參數。

    參數:
    KEYWORDS (str): 職缺的關鍵字。
    CATEGORY (str): 職缺的類別。
    ORDER (str, optional): 排序的參數，預設為 None。

    返回:
    str: 生成的職缺網址。
    """

    BASE_URL = JOB_LISTING_BASE_URL_CAKERESUME
    logger.debug("Using BASE_URL for CakeResume job listing", base_url=BASE_URL)

    if KEYWORDS and CATEGORY:
        url = f"{BASE_URL}/{KEYWORDS}?profession[0]={CATEGORY}&page="
    elif KEYWORDS:
        url = f"{BASE_URL}/{KEYWORDS}?page="
    elif CATEGORY:
        url = f"{BASE_URL}/categories/{CATEGORY}?page="
    else:
        url = f"{BASE_URL}?page="

    if ORDER:  # 只在 ORDER 不為 None 時添加
        url = url.replace("?page=", f"?order={ORDER}&page=")

    return url

def fetch_cakeresume_job_urls(
    KEYWORDS: str,
    CATEGORY: str,
    ORDER: Optional[str] = None,
    PAGE_NUM: int = 0,
) -> Optional[str]: # Returns HTML content of the job listing page
    """
    從 CakeResume 獲取職缺 URL 列表的原始數據 (HTML 內容)。
    """
    url = cake_me_url(KEYWORDS, CATEGORY, ORDER) + str(PAGE_NUM)
    logger.debug("Final URL for CakeResume job listing", final_url=url)
    return _make_web_request(
        "GET",
        url,
        headers=HEADERS_CAKERESUME,
        timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
        verify=False,
        log_context={
            "api_type": "cakeresume_job_urls",
            "keywords": KEYWORDS,
            "category": CATEGORY,
            "page": PAGE_NUM,
        },
    )

def fetch_cakeresume_job_data(job_url: str) -> Optional[str]: # Returns HTML content of the job detail page
    """
    從 CakeResume 職缺頁面抓取單一 URL 的資料 (HTML 內容)。
    """
    return _make_web_request(
        "GET",
        job_url,
        headers=HEADERS_CAKERESUME,
        timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
        verify=False,
        log_context={
            "api_type": "cakeresume_job_detail",
            "url": job_url,
        },
    )


def fetch_cakeresume_company_page_html(company_url: str) -> Optional[str]:
    """
    Fetches the raw HTML content of a CakeResume company page.
    """
    return _make_web_request(
        "GET",
        company_url,
        headers=HEADERS_CAKERESUME,
        timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
        verify=False,
        log_context={
            "api_type": "cakeresume_company_page",
            "url": company_url,
        },
    )

