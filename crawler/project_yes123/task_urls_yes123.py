import os

# python -m crawler.project_yes123.task_urls_yes123
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import structlog
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import urllib3
from typing import Set, List, Optional, Dict
from datetime import datetime, timezone, timedelta
import re
import pandas as pd

from crawler.database.schemas import (
    SourcePlatform,
    CategorySourcePydantic,
    UrlPydantic,
    CrawlStatus,
    JobStatus,
    JobPydantic,
    JobType,
    LocationPydantic,
    SkillPydantic,
    CompanyPydantic,
    JobObservationPydantic,
)
from crawler.database.repository import (
    upsert_urls,
    get_all_categories_for_platform,
    get_all_crawled_category_ids_pandas,
    get_stale_crawled_category_ids_pandas,
    upsert_jobs,
    update_urls_status,
    insert_job_observations,
    upsert_url_categories,
)
from crawler.database.connection import initialize_database
from crawler.project_yes123.config_yes123 import HEADERS_YES123, JOB_LISTING_BASE_URL_YES123
from crawler.utils.salary_parser import parse_salary_text
from crawler.utils.run_skill_extraction import extract_skills_precise, preprocess_skills_for_extraction

logger = structlog.get_logger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Skill master data loading ---
SKILL_MASTER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'utils',
    'skill_data',
    'generated_data',
    'skill_master.json'
)

COMPILED_SKILL_PATTERNS = []
try:
    skill_master_df = pd.read_json(SKILL_MASTER_PATH)
    COMPILED_SKILL_PATTERNS = preprocess_skills_for_extraction(skill_master_df)
    logger.info(f"已載入技能主檔並編譯技能模式: {SKILL_MASTER_PATH}")
except FileNotFoundError:
    logger.error(f"錯誤：找不到技能主檔。請先執行 `python3 -m crawler.utils.run_skill_extraction --generate-kb` 來生成 {SKILL_MASTER_PATH}")
except Exception as e:
    logger.error(f"載入技能主檔或編譯技能模式失敗: {e}")

# --- Constants ---
BASE_URL = "https://www.yes123.com.tw/wk_index/"
JOB_LIST_URL_TEMPLATE = f"{JOB_LISTING_BASE_URL_YES123}?find_work_mode1={{job_category_code}}&order_by=m_date&order_ascend=desc&search_from=joblist"
JOB_LINK_SELECTOR = "div[id^=\"\"][class*=\"Job_opening\"] a.Job_opening_block"
DEFAULT_TIMEOUT = 15

# --- Job Detail Scraping and Parsing Functions (from task_jobs_yes123.py) ---

def fetch_yes123_job_data(job_url: str, headers: dict, timeout: int = DEFAULT_TIMEOUT) -> Optional[dict]:
    """ Fetches and scrapes detailed information from a given yes123 job URL. """
    logger.info("Fetching job data", url=job_url)
    try:
        response = requests.get(job_url, headers=headers, timeout=timeout, verify=False)
        response.raise_for_status() # This raises HTTPError for bad responses (4xx or 5xx)
        logger.info("Received response", url=job_url, status_code=response.status_code)

        if "此工作機會已關閉" in response.text:
            logger.warning("Job is closed", url=job_url)
            return None
        if "您要找的頁面不存在" in response.text:
            logger.warning("Page not found", url=job_url)
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        scraped_data = {"職缺網址": job_url}

        # Extract Title and Company Info
        header_block = soup.select_one("div.box_job_header_center")
        if header_block:
            title_tag = header_block.select_one("h1")
            scraped_data["職缺名稱"] = title_tag.get_text(strip=True) if title_tag else "N/A"
            company_tag = header_block.select_one("a.link_text_black")
            scraped_data["公司名稱"] = company_tag.get_text(strip=True) if company_tag else "N/A"
            if company_tag and "href" in company_tag.attrs:
                company_url = urljoin(BASE_URL, company_tag["href"])
                scraped_data["公司網址"] = company_url
                # Extract p_id as company_id
                if "p_id=" in company_url:
                    scraped_data["公司ID"] = company_url.split("p_id=")[-1].split("&")[0]
                else:
                    scraped_data["公司ID"] = "N/A"
            else:
                scraped_data["公司網址"] = "N/A"
                scraped_data["公司ID"] = "N/A"

        # Extract Posted At
        posted_at_tag = soup.find(string=re.compile(r"職缺更新："))
        if posted_at_tag:
            posted_at_text = posted_at_tag.get_text(strip=True).replace("職缺更新：", "")
            today = datetime.now(timezone.utc)
            
            if "今天" in posted_at_text:
                scraped_data["發布日期"] = today
            elif "昨天" in posted_at_text:
                scraped_data["發布日期"] = today - timedelta(days=1)
            else:
                weekday_map = {
                    "星期一": 0, "星期二": 1, "星期三": 2, "星期四": 3, "星期五": 4, "星期六": 5, "星期日": 6
                }
                found_weekday = False
                for weekday_str, weekday_num in weekday_map.items():
                    if weekday_str in posted_at_text:
                        # Calculate days ago based on current weekday and target weekday
                        # weekday() returns 0 for Monday, 6 for Sunday
                        current_weekday = today.weekday()
                        days_ago = (current_weekday - weekday_num + 7) % 7
                        scraped_data["發布日期"] = today - timedelta(days=days_ago)
                        found_weekday = True
                        break
                
                if not found_weekday:
                    try:
                        current_year = datetime.now(timezone.utc).year
                        # Try parsing YYYY.MM.DD format
                        if re.match(r"^\d{4}\.\d{2}\.\d{2}$", posted_at_text):
                            posted_at_date = datetime.strptime(posted_at_text, "%Y.%m.%d").replace(tzinfo=timezone.utc)
                        # Try parsing MM.DD format, assuming current year
                        elif re.match(r"^\d{2}\.\d{2}$", posted_at_text):
                            posted_at_date = datetime.strptime(f"{current_year}.{posted_at_text}", "%Y.%m.%d").replace(tzinfo=timezone.utc)
                        else:
                            raise ValueError("Unknown date format")
                        scraped_data["發布日期"] = posted_at_date
                    except ValueError:
                        logger.warning("無法解析詳情頁的 posted_at 日期格式", value=posted_at_text, job_url=job_url)
                        scraped_data["發布日期"] = None
        else:
            scraped_data["發布日期"] = None

        # Extract Job Description (工作內容)
        job_description_section = soup.find("h3", string=re.compile(r"徵才說明"))
        if job_description_section:
            # Assuming the description content is in the next sibling div or a specific div
            description_content = job_description_section.find_next_sibling("div")
            if description_content:
                scraped_data["工作內容"] = description_content.get_text(strip=True, separator="\n")
            else:
                # Fallback if not directly in a sibling div, might be in the parent's text
                scraped_data["工作內容"] = job_description_section.parent.get_text(strip=True, separator="\n")
        else:
            scraped_data["工作內容"] = "" # Ensure it's not None if not found

        # Extract other key-value pairs (薪資待遇, 工作性質, 工作地點, 學歷要求, 工作經驗)
        # These are typically in "div.job_explain" sections, but the structure might vary.
        # Let's try to be more robust by looking for common patterns.
        
        # Look for all "div.job_explain" and "div.job_skill" sections
        for section in soup.select("div.job_explain, div.job_skill"):
            # Find all list items within these sections
            for item in section.select("ul > li, div.item"):
                key_tag = item.select_one("span.left_title, div.left") # Added div.left
                value_tag = item.select_one("span.right_main, div.right") # Added div.right
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

def parse_job_details_to_pydantic(job_data: Dict[str, any], url: str, source_category_id: str) -> Optional[JobPydantic]:
    """Parses the scraped job data dictionary and converts it into a JobPydantic object."""
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
        location_text = job_data.get("工作地點", "")
        region = None
        district = None
        latitude = None
        longitude = None

        if location_text:
            # Extract region and district (assuming first 3 chars for region, full text for district)
            if len(location_text) >= 3:
                region = location_text[:3]
            district = location_text[:6] # Extract first 6 characters for district

            # Remove geocoding from here
            # coordinates = geocode_address(location_text)
            # if coordinates:
            #     latitude = str(coordinates["latitude"])
            #     longitude = str(coordinates["longitude"])
        description = job_data.get("工作內容", "")
        extracted_skills = []
        if description and COMPILED_SKILL_PATTERNS:
            extracted_skills = extract_skills_precise(description, COMPILED_SKILL_PATTERNS)
        logger.debug("Parsing job details to Pydantic.", url=url, source_category_id=source_category_id)
        return JobPydantic(
            source_platform=SourcePlatform.PLATFORM_YES123,
            source_job_id=job_id if job_id else url,
            url=job_data.get("職缺網址", url),
            status=JobStatus.ACTIVE,
            title=job_data.get("職缺名稱", ""),
            description=description,
            salary_text=salary_text,
            salary_min=min_salary,
            salary_max=max_salary,
            salary_type=salary_type,
            education_required_text=education_required_text,
            experience_required_text=experience_required_text,
            company=CompanyPydantic(
                source_platform=SourcePlatform.PLATFORM_YES123,
                source_company_id=job_data.get("公司ID", ""), # Use extracted company_id
                name=job_data.get("公司名稱", ""),
                url=job_data.get("公司網址", ""),
            ),
            locations=[LocationPydantic(
                region=region,
                district=district,
                address_detail=location_text,
                latitude=latitude,
                longitude=longitude,
            )],
            skills=[SkillPydantic(name=skill_name) for skill_name in extracted_skills],
            posted_at=job_data.get("發布日期"),
            job_type=_parse_job_type(job_data.get("工作性質")),
            category_tags=[source_category_id],
        )
    except Exception as e:
        logger.error("Failed to parse job data to Pydantic.", error=e, job_data=job_data, url=url, exc_info=True)
        return None

def _upsert_batch_data(
    jobs_for_upsert: List[JobPydantic],
    jobs_for_observations: List[JobObservationPydantic],
    url_category_tags: List[Dict[str, str]],
    db_name: str,
    job_category_code: str # Added job_category_code
):
    """將收集到的批次資料寫入資料庫。"""
    if not jobs_for_observations:
        logger.info("此批次無資料可上傳。", category=job_category_code)
        return

    # 優先插入觀測資料
    insert_job_observations(jobs_for_observations, db_name=db_name)
    logger.info(f"成功插入 {len(jobs_for_observations)} 筆職缺觀測記錄。", category=job_category_code)

    # 如果有新職缺，則更新相關表格
    if jobs_for_upsert:
        urls_to_upsert = [
            UrlPydantic(source_url=job.url, source=job.source_platform)
            for job in jobs_for_upsert
        ]
        upsert_jobs(jobs_for_upsert, db_name=db_name)
        upsert_urls(SourcePlatform.PLATFORM_YES123, urls_to_upsert, db_name=db_name)
        upsert_url_categories(url_category_tags, db_name=db_name)
        logger.info(
            "成功更新批次資料至資料庫。",
            jobs_upserted=len(jobs_for_upsert),
            urls_upserted=len(urls_to_upsert),
            category=job_category_code
        )
    else:
        logger.info("此批次無新職缺可更新。", category=job_category_code)

def _process_single_url(url: str, job_category_code: str, db_name: str):
    """Fetches, parses, and stores data for a single job URL."""
    job_id = None
    logger.debug("Processing single URL", url=url)
    try:
        if not url.startswith(BASE_URL):
            logger.warning("Invalid job URL format, skipping.", url=url)
            update_urls_status([url], CrawlStatus.FAILED, db_name=db_name)
            return
        if "job_id=" in url:
            job_id = url.split("job_id=")[-1]
        elif "p_id=" in url:
            job_id = url.split("p_id=")[-1].split("&")[0]

        # A single URL object for status updates
        url_pydantic = UrlPydantic(source_url=url, source=SourcePlatform.PLATFORM_YES123, source_category_id=job_category_code)
        upsert_urls(SourcePlatform.PLATFORM_YES123, [url_pydantic], db_name=db_name)

        job_data = fetch_yes123_job_data(url, HEADERS_YES123)
        if not job_data:
            logger.warning("fetch_job_data_failed", job_id=job_id, url=url, category=job_category_code)
            update_urls_status([url], CrawlStatus.FAILED, db_name=db_name)
            return

        job_pydantic_data = parse_job_details_to_pydantic(job_data, url, job_category_code)
        if not job_pydantic_data:
            logger.error("job_data_parsing_failed", job_id=job_id, url=url, category=job_category_code)
            update_urls_status([url], CrawlStatus.FAILED, db_name=db_name)
            return

        upsert_jobs([job_pydantic_data], db_name=db_name)

        job_observation = JobObservationPydantic(
            source_job_id=job_pydantic_data.source_job_id,
            source_platform=job_pydantic_data.source_platform,
            url=job_pydantic_data.url,
            title=job_pydantic_data.title,
            description=job_pydantic_data.description,
            job_type=job_pydantic_data.job_type,
            posted_at=job_pydantic_data.posted_at,
            status=job_pydantic_data.status,
            salary_text=job_pydantic_data.salary_text,
            salary_min=job_pydantic_data.salary_min,
            salary_max=job_pydantic_data.salary_max,
            salary_type=job_pydantic_data.salary_type,
            experience_required_text=job_pydantic_data.experience_required_text,
            education_required_text=job_pydantic_data.education_required_text,
            company_id=job_pydantic_data.company.source_company_id,
            company_name=job_pydantic_data.company.name,
            company_url=job_pydantic_data.company.url,
            location_text=job_pydantic_data.locations[0].address_detail if job_pydantic_data.locations else None,
            region=job_pydantic_data.locations[0].region if job_pydantic_data.locations else None,
            district=job_pydantic_data.locations[0].district if job_pydantic_data.locations else None,
            latitude=job_pydantic_data.locations[0].latitude if job_pydantic_data.locations else None,
            longitude=job_pydantic_data.locations[0].longitude if job_pydantic_data.locations else None,
            skills=", ".join([skill.name for skill in job_pydantic_data.skills]) if job_pydantic_data.skills else None,
        )
        insert_job_observations([job_observation], db_name=db_name)

        logger.info("job_upsert_success", job_id=job_id, url=url, category=job_category_code)
        update_urls_status([url], CrawlStatus.SUCCESS, db_name=db_name)

    except Exception as e:
        logger.error("unexpected_url_processing_error", error=str(e), job_id=job_id, url=url, category=job_category_code, exc_info=True)
        update_urls_status([url], CrawlStatus.FAILED, db_name=db_name)

# --- Main Crawler Logic ---

def create_session_with_retries() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS_YES123)
    session.verify = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return session

def task_crawl_yes123_page_and_chain(job_category_code: str, page_num: int, max_page: int = None, db_name: str = None):
    page_url = f"{JOB_LIST_URL_TEMPLATE.format(job_category_code=job_category_code)}&strrec={(page_num - 1) * 30}"
    logger.info("start_processing_page", page_num=page_num, category=job_category_code, max_page=max_page)
    session = create_session_with_retries()
    try:
        response = session.get(page_url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        response.encoding = 'utf-8-sig'
        with open('/home/soldier/crawler_system_v0_local_test/crawler/yes123_response_debug.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info("Saved response HTML for debugging.", file='/home/soldier/crawler_system_v0_local_test/crawler/yes123_response_debug.html', category=job_category_code)
        soup = BeautifulSoup(response.text, "html.parser")

        if max_page is None:
            select_tag = soup.select_one("#inputState")
            if select_tag:
                options = select_tag.find_all("option")
                if options:
                    max_page = int(options[-1]["value"])
                    logger.info("extracted_max_page", max_page=max_page, category=job_category_code)
                else:
                    max_page = page_num
            else:
                max_page = page_num
        
        job_links = [urljoin(BASE_URL, tag["href"]) for tag in soup.select(JOB_LINK_SELECTOR) if "href" in tag.attrs]

        if job_links:
            logger.info("page_found_urls", count=len(job_links), page=page_num, category=job_category_code)
            # Process each link immediately
            for link in job_links:
                _process_single_url(link, job_category_code, db_name)
        else:
            logger.warning("empty_page_no_urls", page=page_num, category=job_category_code)

        if page_num < max_page:
            next_page_num = page_num + 1
            logger.info("chaining_next_page_task", next_page=next_page_num, max_page=max_page, category=job_category_code)
            task_crawl_yes123_page_and_chain(job_category_code=job_category_code, page_num=next_page_num, max_page=max_page, db_name=db_name)
        else:
            logger.info("reached_max_page", page=page_num, max_page=max_page, category=job_category_code)

    except Exception as e:
        logger.error("network_error_web_request", url=page_url, error=str(e), exc_info=True, category=job_category_code)

def task_start_yes123_crawl_chain(job_category: dict, db_name: str = None):
    try:
        category = CategorySourcePydantic.model_validate(job_category)
        job_category_code = category.source_category_id
    except Exception as e:
        logger.error("invalid_job_category_data", data=job_category, error=str(e), exc_info=True)
        return
    logger.info("start_task_chain", job_category_code=job_category_code)
    task_crawl_yes123_page_and_chain(job_category_code=job_category_code, page_num=1, max_page=None, db_name=db_name)

# --- Local Test Runner ---
def _run_local_test():
    db_name = os.getenv("CRAWLER_DB_NAME", "db_YES123")
    initialize_database(db_name=db_name)
    n_days = 7
    logger.info("start_local_test_fetching_categories", platform=SourcePlatform.PLATFORM_YES123)
    all_categories: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_YES123, db_name=db_name)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories}
    crawled_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YES123, db_name=db_name)
    stale_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YES123, n_days, db_name=db_name)
    dispatch_ids = (all_category_ids - crawled_ids) | stale_ids
    categories_to_dispatch = [cat for cat in all_categories if cat.source_category_id in dispatch_ids]

    if not categories_to_dispatch:
        logger.info("no_job_categories_to_process", platform=SourcePlatform.PLATFORM_YES123)
        return

    logger.info("found_categories_to_crawl", count=len(categories_to_dispatch), platform=SourcePlatform.PLATFORM_YES123)
    
    # Custom sort: prioritize categories with parent_source_id starting with '2_1011'
    def custom_category_sort_key(category):
        if category.parent_source_id and str(category.parent_source_id).startswith('2_1011'):
            return (0, category.source_category_name)  # Prioritize
        return (1, category.source_category_name) # Normal sort

    categories_to_dispatch = sorted(categories_to_dispatch, key=custom_category_sort_key) 

    for category in categories_to_dispatch:
        logger.info("local_dispatch_start_task", job_category_code=category.source_category_id)
        task_start_yes123_crawl_chain(category.model_dump(), db_name=db_name)

if __name__ == "__main__":
    _run_local_test()
