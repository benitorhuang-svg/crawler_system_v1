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
from crawler.database.schemas import SourcePlatform
from crawler.project_yourator.config_yourator import (
    HEADERS_YOURATOR,
    JOB_API_BASE_URL_YOURATOR,
    JOB_CAT_URL_YOURATOR,
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
    logger.debug(
        "Sleeping before API request.",
        event="sleeping_before_api_request",
        duration=sleep_time,
        platform=SourcePlatform.PLATFORM_YOURATOR,
        component="client",
        **log_context
    )
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
            event="network_error_api_request",
            url=url,
            error=str(e),
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="client",
            exc_info=True,
            **log_context,
        )
        raise  # Re-raise the exception to trigger tenacity retry
    except json.JSONDecodeError:
        logger.error(
            "Failed to parse JSON response from API.",
            event="json_decode_error_api_response",
            url=url,
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="client",
            exc_info=True,
            **log_context,
        )
        return None
    except Exception as e:
        logger.error(
            "Unexpected error during API request.",
            event="unexpected_error_api_request",
            url=url,
            error=str(e),
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="client",
            exc_info=True,
            **log_context,
        )
        return None

def fetch_category_data_from_yourator_api(
    api_url: str = JOB_CAT_URL_YOURATOR, headers: Dict[str, str] = HEADERS_YOURATOR
) -> Optional[Dict[str, Any]]:
    """
    從 Yourator API 獲取職務分類的原始數據。
    """
    return _make_api_request(
        "GET",
        api_url,
        headers=headers,
        log_context={
            "api_type": "yourator_category_data",
            "platform": SourcePlatform.PLATFORM_YOURATOR,
            "component": "client"
        },
    )

def fetch_job_urls_from_yourator_api(
    page: int = 1,
    category: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    從 Yourator API 獲取職缺 URL 列表的原始數據。
    """
    params = {"page": page}
    if category:
        params["category[]"] = category

    return _make_api_request(
        "GET",
        JOB_API_BASE_URL_YOURATOR,
        headers=HEADERS_YOURATOR,
        params=params,
        timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
        verify=False,
        log_context={
            "api_type": "yourator_job_urls",
            "page": page,
            "category": category,
            "platform": SourcePlatform.PLATFORM_YOURATOR,
            "component": "client"
        },
    )

def fetch_job_data_from_yourator_api(job_id: str) -> Optional[Dict[str, Any]]:
    """
    從 Yourator API 獲取單一職缺的原始數據。
    """
    api_url = f"{JOB_API_BASE_URL_YOURATOR}/{job_id}"
    return _make_api_request(
        "GET",
        api_url,
        headers=HEADERS_YOURATOR,
        timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
        log_context={
            "job_id": job_id,
            "api_type": "yourator_job_data",
            "platform": SourcePlatform.PLATFORM_YOURATOR,
            "component": "client"
        },
    )
