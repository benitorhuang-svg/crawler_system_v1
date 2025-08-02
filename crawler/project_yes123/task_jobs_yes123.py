# import os
# # python -m crawler.project_yes123.task_jobs_yes123
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import structlog
from typing import Optional, Dict
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import re
from urllib.parse import urljoin

import requests
import urllib3

from crawler.worker import app
from crawler.database.schemas import (
    CrawlStatus,
    SourcePlatform,
    JobStatus,
    JobPydantic,
    JobType,
)
from crawler.database.connection import initialize_database
from crawler.database.repository import (
    get_urls_by_crawl_status,
    upsert_jobs,
    mark_urls_as_crawled,
)
from crawler.utils.salary_parser import parse_salary_text
from crawler.project_yes123.config_yes123 import HEADERS_YES123

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Constants
BASE_URL = "https://www.yes123.com.tw"
DEFAULT_TIMEOUT = 15

logger = structlog.get_logger(__name__)


def fetch_yes123_job_data(job_url: str, headers: dict, timeout: int = DEFAULT_TIMEOUT) -> Optional[dict]:
    """
    Fetches and scrapes detailed information from a given yes123 job URL.
    """
    try:
        response = requests.get(job_url, headers=headers, timeout=timeout, verify=False)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response.raise_for_status()

        if "此工作機會已關閉" in response.text or "您要找的頁面不存在" in response.text:
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        scraped_data = {"職缺網址": job_url}
        
        header_block = soup.select_one("div.box_job_header_center")
        if header_block:
            title_tag = header_block.select_one("h1")
            scraped_data["職缺名稱"] = title_tag.get_text(strip=True) if title_tag else "N/A"
            company_tag = header_block.select_one("a.link_text_black")
            scraped_data["公司名稱"] = company_tag.get_text(strip=True) if company_tag else "N/A"
            if company_tag and "href" in company_tag.attrs:
                scraped_data["公司網址"] = urljoin(BASE_URL, company_tag["href"])
            else:
                scraped_data["公司網址"] = "N/A"

        posted_at_tag = soup.find(string=re.compile(r"職缺更新："))
        if posted_at_tag:
            posted_at_text = posted_at_tag.get_text(strip=True).replace("職缺更新：", "")
            if "今天" in posted_at_text:
                scraped_data["發布日期"] = datetime.now(timezone.utc)
            else:
                try:
                    current_year = datetime.now(timezone.utc).year
                    posted_at_date = datetime.strptime(f"{current_year}.{posted_at_text}", "%Y.%m.%d").replace(tzinfo=timezone.utc)
                    scraped_data["發布日期"] = posted_at_date
                except ValueError:
                    try:
                        posted_at_date = datetime.strptime(posted_at_text, "%Y.%m.%d").replace(tzinfo=timezone.utc)
                        scraped_data["發布日期"] = posted_at_date
                    except ValueError:
                        scraped_data["發布日期"] = None
        else:
            scraped_data["發布日期"] = None

        for section in soup.select("div.job_explain"):
            section_title_tag = section.select_one("h3")
            if not section_title_tag:
                continue
            section_title = section_title_tag.get_text(strip=True)

            if section_title in ["徵才說明", "工作條件", "企業福利", "技能與求職專長"]:
                for item in section.select("ul > li"):
                    key_tag = item.select_one("span.left_title")
                    value_tag = item.select_one("span.right_main")
                    if key_tag and value_tag:
                        key = key_tag.get_text(strip=True).replace("：", "")
                        value = value_tag.get_text(strip=True, separator="\n")
                        scraped_data.setdefault(key, "")
                        scraped_data[key] += f"\n(補充) {value}" if scraped_data[key] else value

        return scraped_data

    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch yes123 job data.", url=job_url, error=e)
        return None
    except Exception as e:
        logger.error("An unexpected error occurred during scraping.", url=job_url, error=e, exc_info=True)
        return None


def _parse_job_type(job_nature_text: Optional[str]) -> JobType:
    """Maps job nature text to JobType enum."""
    if not job_nature_text:
        return JobType.OTHER
    if "全職" in job_nature_text:
        return JobType.FULL_TIME
    if "兼職" in job_nature_text:
        return JobType.PART_TIME
    if "工讀" in job_nature_text:
        return JobType.INTERNSHIP
    return JobType.OTHER


def parse_job_details_to_pydantic(job_data: Dict[str, any], url: str) -> Optional[JobPydantic]:
    """
    Parses the scraped job data dictionary and converts it into a JobPydantic object.
    """
    try:
        job_id = None
        if "job_id=" in url:
            job_id = url.split("job_id=")[-1]
        elif "p_id=" in url:
            job_id = url.split("p_id=")[-1].split("&")[0]

        salary_text = job_data.get("薪資待遇", "")
        min_salary, max_salary, salary_type = parse_salary_text(salary_text)

        education_required_text = job_data.get("學歷要求", "") or "不拘"
        experience_required_text = job_data.get("工作經驗", "") or "不拘"

        return JobPydantic(
            source_platform=SourcePlatform.PLATFORM_YES123,
            source_job_id=job_id if job_id else url,
            url=job_data.get("職缺網址", url),
            status=JobStatus.ACTIVE,
            title=job_data.get("職缺名稱", ""),
            description=job_data.get("工作內容", ""),
            salary_text=salary_text,
            salary_min=min_salary,
            salary_max=max_salary,
            salary_type=salary_type,
            location_text=job_data.get("工作地點", ""),
            education_required_text=education_required_text,
            experience_required_text=experience_required_text,
            company_name=job_data.get("公司名稱", ""),
            company_url=job_data.get("公司網址", ""),
            posted_at=job_data.get("發布日期"),
            job_type=_parse_job_type(job_data.get("工作性質")),
        )
    except Exception as e:
        logger.error("Failed to parse job data to Pydantic.", error=e, job_data=job_data, url=url, exc_info=True)
        return None


@app.task()
def fetch_url_data_yes123(url: str) -> Optional[dict]:
    """
    Celery task: Fetches detailed job info from a URL, parses, stores it, and marks the URL status.
    """
    job_id = None
    try:
        if "job_id=" in url:
            job_id = url.split("job_id=")[-1]
        elif "p_id=" in url:
            job_id = url.split("p_id=")[-1].split("&")[0]

        job_data = fetch_yes123_job_data(url, HEADERS_YES123)
        if not job_data:
            logger.warning("Failed to fetch job data or job is closed.", job_id=job_id, url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        job_pydantic_data = parse_job_details_to_pydantic(job_data, url)
        if not job_pydantic_data:
            logger.error("Failed to parse job data.", job_id=job_id, url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        upsert_jobs([job_pydantic_data])
        logger.info("Job parsed and upserted successfully.", job_id=job_id, url=url)
        mark_urls_as_crawled({CrawlStatus.SUCCESS: [url]})
        return job_pydantic_data.model_dump()

    except Exception as e:
        logger.error("Unexpected error processing URL.", error=e, job_id=job_id, url=url, exc_info=True)
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return None


if __name__ == "__main__":
    initialize_database()

    PRODUCER_BATCH_SIZE = 20000000000000
    statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING, CrawlStatus.QUEUED]

    logger.info("Fetching URLs to process for local testing.", statuses=statuses_to_fetch, limit=PRODUCER_BATCH_SIZE)

    urls_to_process = get_urls_by_crawl_status(
        platform=SourcePlatform.PLATFORM_YES123,
        statuses=statuses_to_fetch,
        limit=PRODUCER_BATCH_SIZE,
    )

    if urls_to_process:
        logger.info("Found URLs to process.", count=len(urls_to_process))
        for url in urls_to_process:
            logger.info("Processing URL.", url=url)
            fetch_url_data_yes123(url)
    else:
        logger.info("No URLs found to process for testing.")
