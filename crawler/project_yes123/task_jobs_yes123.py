import os
# python -m crawler.project_yes123.task_jobs_yes123
# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---

import structlog
from typing import Optional
import re
from datetime import datetime
from bs4 import BeautifulSoup

from crawler.worker import app
from crawler.database.schemas import CrawlStatus, SourcePlatform, JobPydantic, JobStatus, JobType, SalaryType
from crawler.database.connection import initialize_database
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled, get_urls_by_crawl_status
from crawler.project_yes123.client_yes123 import fetch_yes123_job_data
from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

# yes123 的工作類型到我們內部 JobType Enum 的映射
# 根據 notebook 中的 '工作性質' 欄位
JOB_TYPE_MAPPING_YES123 = {
    "全職": JobType.FULL_TIME,
    "兼職": JobType.PART_TIME,
    "派遣": JobType.CONTRACT,
    "工讀": JobType.INTERNSHIP, # Assuming工讀 is internship
    "約聘": JobType.TEMPORARY,
    "其他": JobType.OTHER, # Added for cases where job_type is None
}


def parse_salary(
    salary_text: str,
) -> (Optional[int], Optional[int], Optional[SalaryType]):
    salary_min, salary_max, salary_type = None, None, None
    text = salary_text.replace(",", "").lower()

    # 月薪
    match_monthly = re.search(r"月薪([0-9,]+)(?:元)?(?:[至~])?([0-9,]+)?元?", text) or re.search(
        r"月薪([0-9,]+)元以上", text
    )
    if match_monthly:
        salary_type = SalaryType.MONTHLY
        salary_min = int(match_monthly.group(1).replace(",", ""))
        if match_monthly.group(2):
            salary_max = int(match_monthly.group(2).replace(",", ""))
        return salary_min, salary_max, salary_type

    # 年薪
    match_yearly = re.search(r"年薪([0-9,]+)萬(?:[至~])?([0-9,]+)?萬?", text) or re.search(
        r"年薪([0-9,]+)萬以上", text
    )
    if match_yearly:
        salary_type = SalaryType.YEARLY
        salary_min = int(match_yearly.group(1).replace(",", "")) * 10000
        if match_yearly.group(2):
            salary_max = int(match_yearly.group(2).replace(",", "")) * 10000
        return salary_min, salary_max, salary_type

    # 時薪
    match_hourly = re.search(r"時薪([0-9,]+)元", text)
    if match_hourly:
        salary_type = SalaryType.HOURLY
        salary_min = int(match_hourly.group(1).replace(",", ""))
        salary_max = int(match_hourly.group(1).replace(",", ""))
        return salary_min, salary_max, salary_type

    # 日薪
    match_daily = re.search(r"日薪([0-9,]+)元", text)
    if match_daily:
        salary_type = SalaryType.DAILY
        salary_min = int(match_daily.group(1).replace(",", ""))
        salary_max = int(match_daily.group(1).replace(",", ""))
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


def parse_yes123_job_data_to_pydantic(html_content: str, url: str) -> Optional[JobPydantic]:
    """
    從 yes123 職缺頁面的 HTML 內容解析並轉換為 JobPydantic 物件。
    """
    try:
        # yes123 的 job_id 通常是 URL 中的 p_id 參數
        job_id_match = re.search(r'p_id=(\d+)', url)
        job_id = job_id_match.group(1) if job_id_match else None

        if not job_id:
            logger.error("Failed to extract job_id from URL for parsing.", url=url)
            return None

        soup = BeautifulSoup(html_content, 'html.parser')

        # --- Extracting data based on yes123_人力銀行_crawl.ipynb ---
        # This part needs careful mapping from the notebook's parsing logic

        # Title
        title_element = soup.select_one('h1#limit_word_count')
        title = title_element.get_text(strip=True) if title_element else None
        if not title:
            logger.warning("Job title not found or is empty.", url=url, job_id=job_id)
            return None

        # Company Name and URL
        company_name_element = soup.select_one('#content > div.job_content > div.job_title > div.comp_name > a')
        company_name = company_name_element.get_text(strip=True) if company_name_element else None
        company_url = company_name_element['href'] if company_name_element and 'href' in company_name_element.attrs else None
        # yes123 doesn't seem to have a direct company_source_id in the detail page HTML
        company_source_id = None

        # Job Details Table (工作條件)
        job_detail_table = soup.select_one('#content > div.job_content > div.job_detail > div.job_detail_content > div.job_detail_table')
        job_detail_map = {}
        if job_detail_table:
            for row in job_detail_table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    key = th.get_text(strip=True)
                    value = td.get_text(strip=True)
                    job_detail_map[key] = value

        # Description (工作內容)
        description_element = soup.select_one('#content > div.job_content > div.job_detail > div.job_detail_content > div:nth-child(1) > div.job_detail_content_text')
        description = description_element.get_text(separator='\n', strip=True) if description_element else None

        # 補充 description: 如果 description 為空，嘗試從其他地方獲取
        if not description:
            # 嘗試從其他可能的元素中提取描述，例如 job_detail_map 中的某些鍵
            # 這裡需要根據 yes123 網頁的實際結構來判斷
            # 這裡的 job_detail_map 已經在上面初始化並填充，所以可以使用
            description = job_detail_map.get('工作內容') or job_detail_map.get('職務說明')

        # Extracting from job_detail_map
        salary_text = job_detail_map.get('薪資待遇')
        salary_min, salary_max, salary_type = parse_salary(salary_text or "")

        job_type_str = job_detail_map.get('工作性質')
        job_type = JOB_TYPE_MAPPING_YES123.get(job_type_str) if job_type_str else None
        if job_type is None:
            job_type = JobType.OTHER # Default to OTHER if not mapped

        location_text = job_detail_map.get('工作地點')
        experience_required_text = job_detail_map.get('工作經驗')
        education_required_text = job_detail_map.get('學歷要求')

        # 將 None 轉換為 "不拘"
        if experience_required_text is None:
            experience_required_text = "不拘"
        if education_required_text is None:
            education_required_text = "不拘"

        # Posted At (發佈日期)
        posted_at = None
        posted_at_text = job_detail_map.get('刊登日期')
        if posted_at_text:
            try:
                # Assuming format like '2025/07/30'
                posted_at = datetime.strptime(posted_at_text, "%Y/%m/%d")
            except ValueError:
                logger.warning("Could not parse posted_at date format.", posted_at_text=posted_at_text, job_id=job_id)

        job_pydantic_data = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_YES123,
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
            company_source_id=company_source_id,
            company_name=company_name,
            company_url=company_url,
        )
        return job_pydantic_data

    except Exception as e:
        logger.error(
            "Unexpected error when parsing yes123 job HTML to Pydantic.",
            error=e,
            url=url,
            exc_info=True,
        )
        return None


@app.task()
def fetch_url_data_yes123(url: str) -> Optional[dict]:
    job_id = None
    try:
        job_id_match = re.search(r'p_id=(\d+)', url)
        job_id = job_id_match.group(1) if job_id_match else None

        if not job_id:
            logger.error("Failed to extract job_id from URL.", url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        html_content = fetch_yes123_job_data(url)
        if html_content is None:
            logger.error("Failed to fetch job data from yes123 web.", job_id=job_id, url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        job_pydantic_data = parse_yes123_job_data_to_pydantic(html_content, url)

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
            "Unexpected error when processing yes123 job data.",
            error=e,
            job_id=job_id if 'job_id' in locals() else "N/A",
            url=url,
            exc_info=True,
        )
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return None


if __name__ == "__main__":
    initialize_database()

    statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING, CrawlStatus.QUEUED]
    urls_to_process = get_urls_by_crawl_status(
        platform=SourcePlatform.PLATFORM_YES123,
        statuses=statuses_to_fetch,
        limit=10,
    )
    for url in urls_to_process:
        logger.info("Starting to process URL from the database.", url=url)
        fetch_url_data_yes123(url)