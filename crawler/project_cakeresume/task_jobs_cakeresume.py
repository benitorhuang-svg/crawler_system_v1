import os
# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---

import structlog
from typing import Optional
import re
import json
from bs4 import BeautifulSoup
from datetime import datetime, timezone

from crawler.logging_config import configure_logging
from crawler.worker import app
from crawler.database.schemas import (
    CrawlStatus,
    SourcePlatform,
    JobPydantic,
    JobStatus,
    JobType,
    SalaryType,
)
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled, get_urls_by_crawl_status
from crawler.project_cakeresume.client_cakeresume import fetch_cakeresume_job_data

configure_logging()
logger = structlog.get_logger(__name__)

# CakeResume 的 job_type 到我們內部 JobType Enum 的映射
JOB_TYPE_MAPPING_CAKERESUME = {
    "full-time": JobType.FULL_TIME,
    "part-time": JobType.PART_TIME,
    "internship": JobType.INTERNSHIP,
    "contract": JobType.CONTRACT,
    "other": JobType.OTHER,
}

# CakeResume 的 salary_type 到我們內部 SalaryType Enum 的映射
SALARY_TYPE_MAPPING_CAKERESUME = {
    "monthly": SalaryType.MONTHLY,
    "yearly": SalaryType.YEARLY,
    "hourly": SalaryType.HOURLY,
    "daily": SalaryType.DAILY,
}

def clean_html_if_string(value):
    if isinstance(value, str):
        return BeautifulSoup(value, "html.parser").get_text(separator=' ', strip=True)
    return value


def parse_cakeresume_job_data_to_pydantic(job_details: dict, url: str) -> Optional[JobPydantic]:
    try:
        job_id_match = re.search(r'/jobs/([a-zA-Z0-9]+)', url)
        job_id = job_id_match.group(1) if job_id_match else None

        if not job_id:
            logger.error("Failed to extract job_id from URL for parsing.", url=url)
            return None

        title = job_details.get('title')
        description = clean_html_if_string(job_details.get('description'))
        if not description:
            description = clean_html_if_string(job_details.get('job_responsibilities'))
        if not description:
            description = clean_html_if_string(job_details.get('requirements'))

        job_type_raw = job_details.get('job_type')
        job_type = JOB_TYPE_MAPPING_CAKERESUME.get(job_type_raw)
        if job_type is None:
            job_type = JobType.OTHER

        locations = job_details.get('locations', [])
        location_text = ', '.join(loc.get('name') for loc in locations if loc.get('name')) if locations else None

        posted_at = None
        created_at_str = job_details.get('created_at')
        if created_at_str:
            try:
                posted_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')).astimezone(timezone.utc).replace(tzinfo=None)
            except ValueError:
                logger.warning("Could not parse posted_at date format.", created_at=created_at_str, job_id=job_id)

        salary_min = job_details.get('salary_min')
        salary_max = job_details.get('salary_max')
        salary_type_raw = job_details.get('salary_type')
        salary_type = SALARY_TYPE_MAPPING_CAKERESUME.get(salary_type_raw)
        salary_currency = job_details.get('salary_currency')
        salary_text = f"{salary_currency} {salary_min}-{salary_max} ({salary_type_raw})" if salary_min and salary_max else None
        if job_details.get('hide_salary_completely'):
            salary_text = "面議"
            salary_type = SalaryType.NEGOTIABLE
            salary_min = None
            salary_max = None

        experience_required_text = job_details.get('seniority_level')
        min_work_exp_year = job_details.get('min_work_exp_year')
        if min_work_exp_year is not None:
            experience_required_text = f"{min_work_exp_year} 年以上" if min_work_exp_year > 0 else "不拘"
        
        if experience_required_text is None:
            experience_required_text = "不拘"

        education_required_text = None
        if education_required_text is None:
            education_required_text = "不拘"

        company_info = job_details.get('company', {})
        company_source_id = company_info.get('id')
        company_name = company_info.get('name')
        company_url = company_info.get('website_url')

        job_pydantic_data = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_CAKERESUME,
            source_job_id=job_id,
            url=url,
            status=JobStatus.ACTIVE,
            title=title,
            description=description,
            job_type=job_type,
            location_text=location_text,
            posted_at=posted_at,
            salary_text=salary_text,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
            experience_required_text=experience_required_text,
            education_required_text=education_required_text,
            company_source_id=str(company_source_id) if company_source_id else None,
            company_name=company_name,
            company_url=company_url,
        )
        return job_pydantic_data

    except Exception as e:
        logger.error(
            "Unexpected error when parsing CakeResume job data to Pydantic.",
            error=e,
            url=url,
            exc_info=True,
        )
        return None
logger = structlog.get_logger(__name__)


@app.task()
def fetch_url_data_cakeresume(url: str) -> Optional[dict]:
    job_id = None
    try:
        job_id_match = re.search(r'/jobs/([a-zA-Z0-9]+)', url)
        job_id = job_id_match.group(1) if job_id_match else None

        if not job_id:
            logger.error("Failed to extract job_id from URL.", url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        html_content = fetch_cakeresume_job_data(url)
        if html_content is None:
            logger.error("Failed to fetch job data from CakeResume web.", job_id=job_id, url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        soup = BeautifulSoup(html_content, 'html.parser')
        data_script = soup.find('script', id='__NEXT_DATA__')

        if not data_script:
            logger.error("Error: Could not find job data (script#__NEXT_DATA__) on the page.", url=url, job_id=job_id)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        page_props = json.loads(data_script.string)['props']['pageProps']
        job_details = page_props.get('job')

        if not job_details:
            logger.error("Error: Could not parse job data ('job' key not found) from JSON.", url=url, job_id=job_id)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        job_pydantic_data = parse_cakeresume_job_data_to_pydantic(job_details, url)

        if not job_pydantic_data:
            logger.error(
                "Failed to parse job data to Pydantic.",
                job_id=job_id,
                url=url,
            )
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        upsert_jobs([job_pydantic_data])

        logger.info("Job parsed and upserted successfully.", job_id=job_id, url=url)
        mark_urls_as_crawled({CrawlStatus.SUCCESS: [url]})
        return job_pydantic_data.model_dump()

    except Exception as e:
        logger.error(
            "Unexpected error when processing CakeResume job data.",
            error=e,
            job_id=job_id if 'job_id' in locals() else "N/A",
            url=url,
            exc_info=True,
        )
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return None


if __name__ == "__main__":
    # To run this script for local testing, execute:
    # python -m crawler.project_cakeresume.task_jobs_cakeresume
    # This will automatically use the 'test_db' as configured at the top of the script.

    from crawler.database.connection import initialize_database
    initialize_database()

    statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING, CrawlStatus.QUEUED]
    urls_to_process = get_urls_by_crawl_status(
        platform=SourcePlatform.PLATFORM_CAKERESUME,
        statuses=statuses_to_fetch,
        limit=10,
    )
    for url in urls_to_process:
        logger.info("Starting to process URL from the database.", url=url)
        fetch_url_data_cakeresume(url)