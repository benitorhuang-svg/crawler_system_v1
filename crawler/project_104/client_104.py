import json
import random
import time
from typing import Any, Dict, Optional

import requests
import structlog
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from crawler.config import (
    URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
    URL_CRAWLER_SLEEP_MAX_SECONDS,
    URL_CRAWLER_SLEEP_MIN_SECONDS,
)
from crawler.logging_config import configure_logging
from crawler.project_104.config_104 import (
    HEADERS_104_JOB_API,
    JOB_API_BASE_URL_104,
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
    verify: bool = True,
    log_context: Optional[Dict[str, Any]] = None,
    session: Optional[requests.Session] = None, # Add session parameter
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
        requester = session if session else requests # Use session if provided
        response = requester.request(
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


def fetch_job_data_from_104_api(job_id: str, session: Optional[requests.Session] = None) -> Optional[Dict[str, Any]]:
    """
    從 104 API 獲取單一職缺的原始數據。
    """
    api_url = f"{JOB_API_BASE_URL_104}{job_id}"
    return _make_api_request(
        "GET",
        api_url,
        headers=HEADERS_104_JOB_API,
        timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,  # 加上這行
        log_context={"job_id": job_id, "api_type": "job_data"},
        session=session, # Pass session
    )


def fetch_category_data_from_104_api(
    api_url: str, headers: Dict[str, str], session: Optional[requests.Session] = None
) -> Optional[Dict[str, Any]]:
    """
    從 104 API 獲取職務分類的原始數據。
    """
    return _make_api_request(
        "GET",
        api_url,
        headers=headers,
        log_context={"api_type": "category_data"},
        session=session, # Pass session
    )


def fetch_job_urls_from_104_api(
    base_url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    timeout: int,
    verify: bool = True,
    session: Optional[requests.Session] = None, # Add session parameter
) -> Optional[Dict[str, Any]]:
    """
    從 104 API 獲取職缺 URL 列表的原始數據。
    """
    return _make_api_request(
        "GET",
        base_url,
        headers=headers,
        params=params,
        timeout=timeout,
        verify=verify,
        log_context={"api_type": "job_urls"},
        session=session, # Pass session
    )