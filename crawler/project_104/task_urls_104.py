import random
import time
import structlog
import sys
from collections import deque

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from crawler.worker import app
from crawler.database.models import SourcePlatform
from crawler.database.repository import upsert_urls
from crawler.logging_config import configure_logging
from crawler.config import (
    URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
    URL_CRAWLER_UPLOAD_BATCH_SIZE,
    URL_CRAWLER_SLEEP_MIN_SECONDS,
    URL_CRAWLER_SLEEP_MAX_SECONDS,
)
from crawler.project_104.config_104 import (
    URL_CRAWLER_BASE_URL_104,
    URL_CRAWLER_PAGE_SIZE_104,
    URL_CRAWLER_ORDER_BY_104,
    HEADERS_104_URL_CRAWLER,
)

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

configure_logging()
logger = structlog.get_logger(__name__)

@app.task
def crawl_and_store_category_urls(job_category_code: str) -> None:
    """
    Celery 任務：遍歷指定職缺類別的所有頁面，抓取職缺網址，並將其儲存到資料庫。
    """
    global_job_url_set = set()
    current_batch_urls = []
    recent_counts = deque(maxlen=4)

    current_page = 1
    logger.info("Task started: crawling job category URLs.", job_category_code=job_category_code)

    while True:
        if current_page % 5 == 1:
            logger.info("Current page being processed.", page=current_page, job_category_code=job_category_code)

        params = {
            'jobsource': 'index_s',
            'page': current_page,
            'pagesize': URL_CRAWLER_PAGE_SIZE_104,
            'order': URL_CRAWLER_ORDER_BY_104,
            'jobcat': job_category_code,
            'mode': 's',
            'searchJobs': '1',
        }

        try:
            response = requests.get(
                URL_CRAWLER_BASE_URL_104,
                headers=HEADERS_104_URL_CRAWLER,
                params=params,
                timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
                verify=False
            )
            response.raise_for_status()
            api_data = response.json()

            api_job_urls = api_data.get('data')
            if not isinstance(api_job_urls, list):
                logger.error(
                    "API response 'data' format is incorrect or missing.",
                    page=current_page,
                    job_category_code=job_category_code,
                    api_data_type=type(api_job_urls),
                    api_data_sample=str(api_job_urls)[:100]
                )
                break

            for job_url_item in api_job_urls:
                job_link = job_url_item.get('link', {}).get('job')
                if job_link:
                    if job_link not in global_job_url_set:
                        global_job_url_set.add(job_link)
                        current_batch_urls.append(job_link)

            if len(current_batch_urls) >= URL_CRAWLER_UPLOAD_BATCH_SIZE:
                logger.info("Batch upload size reached. Starting upload.", count=len(current_batch_urls), job_category_code=job_category_code)
                upsert_urls(SourcePlatform.PLATFORM_104, current_batch_urls)
                current_batch_urls.clear()

        except requests.exceptions.HTTPError as http_err:
            logger.error("HTTP error occurred.", error=str(http_err), page=current_page, job_category_code=job_category_code, exc_info=True)
            break
        except requests.exceptions.ConnectionError as conn_err:
            logger.error("Connection error occurred.", error=str(conn_err), page=current_page, job_category_code=job_category_code, exc_info=True)
            break
        except requests.exceptions.Timeout as timeout_err:
            logger.error("Request timeout error occurred.", error=str(timeout_err), page=current_page, job_category_code=job_category_code, exc_info=True)
            break
        except requests.exceptions.RequestException as req_err:
            logger.error("Unknown request error occurred.", error=str(req_err), page=current_page, job_category_code=job_category_code, exc_info=True)
            break
        except ValueError:
            logger.error("Failed to decode JSON response.", page=current_page, job_category_code=job_category_code, exc_info=True)
        except Exception as e:
            logger.error("An unexpected error occurred while fetching page.", error=str(e), page=current_page, job_category_code=job_category_code, exc_info=True)

        total_jobs = len(global_job_url_set)
        recent_counts.append(total_jobs)
        if len(recent_counts) == recent_counts.maxlen and len(set(recent_counts)) == 1:
            logger.info("No new data found consecutively. Ending task early.", max_len=recent_counts.maxlen, job_category_code=job_category_code)
            break

        time.sleep(random.uniform(URL_CRAWLER_SLEEP_MIN_SECONDS, URL_CRAWLER_SLEEP_MAX_SECONDS))

        current_page += 1

    if current_batch_urls:
        logger.info("Task completed. Storing remaining raw job URLs to database.", count=len(current_batch_urls), job_category_code=job_category_code)
        upsert_urls(SourcePlatform.PLATFORM_104, current_batch_urls)
    else:
        logger.info("Task completed. No URLs collected, skipping database storage.", job_category_code=job_category_code)

    logger.info("Task execution finished.", job_category_code=job_category_code)

# if __name__ == '__main__':
#     import sys
#     if len(sys.argv) < 2:
#         logger.info("Usage: python -m crawler.project_104.task_urls_104 <job_category_code>")
#         sys.exit(1)

#     JOBCAT_CODE_FOR_TEST = sys.argv[1]
#     logger.info("Dispatching crawl_and_store_category_urls task for local testing.", job_category_code=JOBCAT_CODE_FOR_TEST)
#     crawl_and_store_category_urls.delay(JOBCAT_CODE_FOR_TEST)