import requests
import sys
import structlog
import json

from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException, JSONDecodeError

from crawler.logging_config import configure_logging
from crawler.project_104.config_104 import HEADERS_104_JOB_API, JOB_API_BASE_URL_104 # Changed import path

configure_logging()
logger = structlog.get_logger(__name__)

def fetch_url_data_104(url: str) -> dict:
    """
    從 104 職缺 API 抓取單一 URL 的資料。
    """
    job_id = url.split('/')[-1].split('?')[0]
    url_api = f'{JOB_API_BASE_URL_104}{job_id}'

    logger.info("Fetching data for single URL.", url=url, job_id=job_id, api_url=url_api)

    try:
        response = requests.get(url_api, headers=HEADERS_104_JOB_API, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info("Successfully fetched data.", job_id=job_id, data_keys=list(data.keys()))
        return data
    except (HTTPError, ConnectionError, Timeout, RequestException) as e:
        logger.error("Network or request error occurred.", url=url, api_url=url_api, error=e, exc_info=True)
        return {}
    except JSONDecodeError as e:
        logger.error("Failed to decode JSON response.", url=url, api_url=url_api, error=e, exc_info=True)
        return {}
    except Exception as e:
        logger.error("An unexpected error occurred.", url=url, api_url=url_api, error=e, exc_info=True)
        return {}

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.info("Usage: python -m crawler.project_104.single_url_api_data_104 <job_url>")
        sys.exit(1)

    job_url = sys.argv[1]
    data = fetch_url_data_104(job_url)
    if data:
        logger.info("Fetched data content (sample).", job_url=job_url, data_sample=json.dumps(data, indent=2, ensure_ascii=False)[:500])
    else:
        logger.warning("No data fetched for the given URL.", job_url=job_url)
