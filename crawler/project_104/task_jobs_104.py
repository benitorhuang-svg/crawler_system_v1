import json
import requests
import structlog
import time
import random
from crawler.worker import app
from crawler.database.models import SourcePlatform, JobPydantic, JobStatus
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled

from typing import Optional
import re
from datetime import datetime
from crawler.database.models import SalaryType, CrawlStatus, JobType
from crawler.config import URL_CRAWLER_SLEEP_MIN_SECONDS, URL_CRAWLER_SLEEP_MAX_SECONDS # Import sleep settings

from crawler.logging_config import configure_logging
from crawler.project_104.config_104 import HEADERS_104_JOB_API, JOB_API_BASE_URL_104 # Changed import path

configure_logging()
logger = structlog.get_logger(__name__)

# 104 API 的 jobType 到我們內部 JobType Enum 的映射
JOB_TYPE_MAPPING = {
    1: JobType.FULL_TIME,
    2: JobType.PART_TIME,
    3: JobType.INTERNSHIP,
    4: JobType.CONTRACT, # 派遣
    5: JobType.TEMPORARY, # 兼職/計時
}

def parse_salary(salary_text: str) -> (Optional[int], Optional[int], Optional[SalaryType]):
    salary_min, salary_max, salary_type = None, None, None
    text = salary_text.replace(",", "").lower()

    # 月薪
    match_monthly = re.search(r'月薪([0-9]+)(?:[至~])([0-9]+)元', text) or re.search(r'月薪([0-9]+)元以上', text)
    if match_monthly:
        salary_type = SalaryType.MONTHLY
        salary_min = int(match_monthly.group(1))
        if len(match_monthly.groups()) > 1 and match_monthly.group(2):
            salary_max = int(match_monthly.group(2))
        return salary_min, salary_max, salary_type

    # 年薪
    match_yearly = re.search(r'年薪([0-9]+)萬(?:[至~])([0-9]+)萬', text) or re.search(r'年薪([0-9]+)萬以上', text)
    if match_yearly:
        salary_type = SalaryType.YEARLY
        salary_min = int(match_yearly.group(1)) * 10000
        if len(match_yearly.groups()) > 1 and match_yearly.group(2):
            salary_max = int(match_yearly.group(2)) * 10000
        return salary_min, salary_max, salary_type

    # 時薪
    match_hourly = re.search(r'時薪([0-9]+)元', text)
    if match_hourly:
        salary_type = SalaryType.HOURLY
        salary_min = int(match_hourly.group(1))
        salary_max = int(match_hourly.group(1))
        return salary_min, salary_max, salary_type

    # 日薪
    match_daily = re.search(r'日薪([0-9]+)元', text)
    if match_daily:
        salary_type = SalaryType.DAILY
        salary_min = int(match_daily.group(1))
        salary_max = int(match_daily.group(1))
        return salary_min, salary_max, salary_type

    # 論件計酬
    if "論件計酬" in text:
        salary_type = SalaryType.BY_CASE
        return None, None, salary_type

    # 面議
    if "面議" in text:
        salary_type = SalaryType.NEGOTIABLE
        return None, None, salary_type

    return salary_min, salary_max, salary_type

@app.task()
def fetch_url_data_104(url: str) -> Optional[JobPydantic]:
    try:
        job_id = url.split('/')[-1].split('?')[0]
        if not job_id:
            logger.error("Failed to extract job_id from URL.", url=url)
            return None

        api_url = f'{JOB_API_BASE_URL_104}{job_id}'

        logger.info("Fetching job data.", job_id=job_id, source_url=api_url)

        # Add random delay before making API request
        sleep_time = random.uniform(URL_CRAWLER_SLEEP_MIN_SECONDS, URL_CRAWLER_SLEEP_MAX_SECONDS)
        logger.debug("Sleeping before API request.", duration=sleep_time)
        time.sleep(sleep_time)

        response = requests.get(api_url, headers=HEADERS_104_JOB_API, timeout=10)
        response.raise_for_status()
        data = response.json()

    except requests.exceptions.RequestException as e:
        logger.error("Network error when requesting API.", url=api_url, error=e, exc_info=True)
        return None
    except json.JSONDecodeError:
        logger.error("Failed to parse JSON response.", url=api_url, exc_info=True)
        return None

    job_data = data.get('data')
    if not job_data or job_data.get('switch') == "off":
        logger.warning("Job content does not exist or is closed.", job_id=job_id)
        return None

    try:
        header = job_data.get('header', {})
        job_detail = job_data.get('jobDetail', {})
        condition = job_data.get('condition', {})

        job_addr_region = job_detail.get('addressRegion', '')
        job_address_detail = job_detail.get('addressDetail', '')
        location_text = (job_addr_region + job_address_detail).strip()
        if not location_text:
            location_text = None

        posted_at = None
        appear_date_str = header.get('appearDate')
        if appear_date_str:
            try:
                posted_at = datetime.strptime(appear_date_str, '%Y/%m/%d')
            except ValueError:
                logger.warning("Could not parse posted_at date format.", appear_date=appear_date_str, job_id=job_id)

        salary_min, salary_max, salary_type = parse_salary(job_detail.get('salary', ''))

        # 處理 job_type 轉換
        raw_job_type = job_detail.get('jobType')
        job_type = JOB_TYPE_MAPPING.get(raw_job_type) if raw_job_type else None

        job_pydantic_data = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_job_id=job_id,
            url=url,
            status=JobStatus.ACTIVE,
            title=header.get('jobName'),
            description=job_detail.get('jobDescription'),
            job_type=job_type, # 使用轉換後的值
            location_text=location_text,
            posted_at=posted_at,
            salary_text=job_detail.get('salary'),
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
            experience_required_text=condition.get('workExp'),
            education_required_text=condition.get('edu'),
            company_source_id=header.get('custNo'),
            company_name=header.get('custName'),
            company_url=header.get('custUrl'),
        )

        upsert_jobs([job_pydantic_data])
        logger.info("Successfully parsed and upserted job.", job_title=job_pydantic_data.title, job_id=job_id)
        mark_urls_as_crawled({CrawlStatus.SUCCESS: [url]}) # 使用 SUCCESS 狀態
        return job_pydantic_data.model_dump()

    except (AttributeError, KeyError) as e:
        logger.error("Missing key fields when parsing data.", error=e, job_id=job_id, exc_info=True)
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return {}
    except Exception as e:
        logger.error("Unexpected error when processing job data.", error=e, job_id=job_id, exc_info=True)
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return {}

# if __name__ == "__main__":
#     from crawler.database.connection import initialize_database
#     from crawler.database.repository import get_unprocessed_urls

#     initialize_database()
#     logger.info("Local testing task_jobs_104. Fetching unprocessed URLs from database.")

#     urls_to_test = get_unprocessed_urls(SourcePlatform.PLATFORM_104, 5)

#     if urls_to_test:
#         for url_obj in urls_to_test:
#             logger.info("Dispatching test URL task.", url=url_obj.source_url)
#             fetch_url_data_104.delay(url_obj.source_url)
#     else:
#         logger.info("No unprocessed URLs available for testing. Please run task_urls_104 first to populate data.")
