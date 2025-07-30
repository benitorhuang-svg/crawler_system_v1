import json
import requests
import structlog
import time
import random
from typing import Optional, Dict, Any

from crawler.config import (
    URL_CRAWLER_SLEEP_MIN_SECONDS,
    URL_CRAWLER_SLEEP_MAX_SECONDS,
    URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
)
from crawler.project_104.config_104 import HEADERS_104_JOB_API, JOB_API_BASE_URL_104
from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

def _make_api_request(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
    verify: bool = True,
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
        return None
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


def fetch_job_data_from_104_api(job_id: str) -> Optional[Dict[str, Any]]:
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
    )


def fetch_category_data_from_104_api(
    api_url: str, headers: Dict[str, str]
) -> Optional[Dict[str, Any]]:
    """
    從 104 API 獲取職務分類的原始數據。
    """
    return _make_api_request(
        "GET",
        api_url,
        headers=headers,
        log_context={"api_type": "category_data"},
    )


def fetch_job_urls_from_104_api(
    base_url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    timeout: int,
    verify: bool = False,
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
    )