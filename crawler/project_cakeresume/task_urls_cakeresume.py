import os

# python -m crawler.project_cakeresume.task_urls_cakeresume
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import structlog

import json
from bs4 import BeautifulSoup
from typing import Set, List, Optional
import re
import pandas as pd

from crawler.worker import app
from crawler.database.schemas import SourcePlatform, CategorySourcePydantic, CrawlStatus, JobObservationPydantic, UrlPydantic
from crawler.database.repository import (
    upsert_urls,
    get_all_categories_for_platform,
    get_all_crawled_category_ids_pandas,
    get_stale_crawled_category_ids_pandas,
    upsert_jobs,
    update_urls_status,
    insert_job_observations,
)
from crawler.project_cakeresume.client_cakeresume import fetch_cakeresume_job_urls, fetch_cakeresume_job_data
from crawler.project_cakeresume.parser_cakeresume import parse_job_details_to_pydantic
from crawler.database.connection import initialize_database
from crawler.config import (
    get_db_name_for_platform,
)
from crawler.project_cakeresume.config_cakeresume import (
    URL_CRAWLER_ORDER_BY_CAKERESUME,
    JOB_DETAIL_BASE_URL_CAKERESUME,
)
from crawler.utils.run_skill_extraction import preprocess_skills_for_extraction

logger = structlog.get_logger(__name__)

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

DEFAULT_TIMEOUT = 15

def _parse_job_urls(soup: BeautifulSoup, current_page: int) -> List[str]:
    """從 HTML 中解析出職缺的 URL 列表。"""
    urls = []
    
    # 嘗試從 __NEXT_DATA__ 中解析
    next_data_script = soup.find('script', id='__NEXT_DATA__')
    if next_data_script:
        try:
            data = json.loads(next_data_script.string)
            # 根據 CakeResume 的 __NEXT_DATA__ 結構，職位資訊通常在 pageProps.serverState.initialResults.Job.results[0].hits
            results = data.get('props', {}).get('pageProps', {}).get('serverState', {}).get('initialResults', {}).get('Job', {}).get('results', [{}])[0].get('hits', [])
            for job in results:
                if 'path' in job and 'page' in job and 'path' in job['page']:
                    full_url = f"{JOB_DETAIL_BASE_URL_CAKERESUME}/companies/{job['page']['path']}/jobs/{job['path']}"
                    urls.append(full_url)
            if urls:
                logger.debug("Parsed URLs from __NEXT_DATA__.", count=len(urls), page=current_page)
                return urls
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Could not parse __NEXT_DATA__ JSON or key not found, falling back to HTML parsing.", error=str(e), page=current_page)

    # 回退到直接從 HTML 連結解析
    job_links = soup.find_all('a', class_='JobSearchItem_jobTitle__bu6yO')
    for link in job_links:
        href = link.get('href')
        if href:
            if href.startswith('http'):
                full_url = href
            else:
                full_url = f"{JOB_DETAIL_BASE_URL_CAKERESUME}{href}"
            urls.append(full_url)
    
    if urls:
        logger.debug("Parsed URLs from HTML links.", count=len(urls), page=current_page)
    else:
        logger.info("No URLs found from HTML links.", page=current_page)

    return urls

def _process_single_job_url(url: str, job_category_code: str, db_name: str):
    """Fetches, parses, and stores data for a single job URL, including URL and category tags."""
    job_id = None
    try:
        match = re.search(r'/jobs/([a-zA-Z0-9_-]+)', url)
        job_id = match.group(1) if match else None

        if not job_id:
            logger.error("Failed to extract job_id from URL.", url=url)
            update_urls_status([url], CrawlStatus.FAILED, db_name=db_name)
            return

        # Upsert URL immediately
        upsert_urls(SourcePlatform.PLATFORM_CAKERESUME, [UrlPydantic(source_url=url, source=SourcePlatform.PLATFORM_CAKERESUME, source_category_id=job_category_code)], db_name=db_name)

        html_content = fetch_cakeresume_job_data(url)
        if not html_content:
            logger.error("Failed to fetch job data from CakeResume.", job_id=job_id, url=url)
            update_urls_status([url], CrawlStatus.FAILED, db_name=db_name)
            return

        soup = BeautifulSoup(html_content, 'html.parser')
        data_script = soup.find('script', id='__NEXT_DATA__')

        if not data_script:
            logger.error("Could not find __NEXT_DATA__ script tag.", url=url, job_id=job_id)
            update_urls_status([url], CrawlStatus.FAILED, db_name=db_name)
            return

        page_props = json.loads(data_script.string).get('props', {}).get('pageProps', {})
        job_details = page_props.get('job')

        if not job_details:
            logger.error("Could not find job details in __NEXT_DATA__.", url=url, job_id=job_id)
            update_urls_status([url], CrawlStatus.FAILED, db_name=db_name)
            return
        
        logger.debug("Raw job details from __NEXT_DATA__.", job_details=job_details, job_id=job_id)
        job_pydantic_data = parse_job_details_to_pydantic(job_details, html_content, url, job_category_code)

        if not job_pydantic_data:
            logger.error("Failed to parse job data to Pydantic.", job_id=job_id, url=url)
            update_urls_status([url], CrawlStatus.FAILED, db_name=db_name)
            return

        upsert_jobs([job_pydantic_data], db_name=db_name)

        # Insert into tb_job_observations
        job_observations = []
        job_observations.append(JobObservationPydantic(
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
            company_id=job_pydantic_data.company.source_company_id if job_pydantic_data.company else None,
            company_name=job_pydantic_data.company.name if job_pydantic_data.company else None,
            company_url=job_pydantic_data.company.url if job_pydantic_data.company else None,
            location_text=job_pydantic_data.locations[0].address_detail if job_pydantic_data.locations else None,
            region=job_pydantic_data.locations[0].region if job_pydantic_data.locations else None,
            district=job_pydantic_data.locations[0].district if job_pydantic_data.locations else None,
            latitude=job_pydantic_data.locations[0].latitude if job_pydantic_data.locations else None,
            longitude=job_pydantic_data.locations[0].longitude if job_pydantic_data.locations else None,
            skills=", ".join([skill.name for skill in job_pydantic_data.skills]) if job_pydantic_data.skills else None,
        ))
        insert_job_observations(job_observations, db_name=db_name)

        logger.info("Job parsed and upserted successfully.", job_id=job_id, url=url)
        update_urls_status([url], CrawlStatus.SUCCESS, db_name=db_name)

    except Exception as e:
        logger.error("Unexpected error processing CakeResume job data.", error=e, job_id=job_id, url=url, exc_info=True)
        update_urls_status([url], CrawlStatus.FAILED, db_name=db_name)

@app.task
def task_crawl_cakeresume_page_and_chain(job_category_code: str, page_num: int, max_page: Optional[int] = None, db_name: Optional[str] = None):
    logger.info("start_processing_page", page_num=page_num, category=job_category_code, max_page=max_page)

    if max_page is not None and page_num > max_page:
        logger.info(
            "Reached max page limit. Ending task early.",
            current_page=page_num,
            max_page=max_page
        )
        return

    html_content = fetch_cakeresume_job_urls(
        KEYWORDS="",
        CATEGORY=job_category_code,
        ORDER=URL_CRAWLER_ORDER_BY_CAKERESUME,
        PAGE_NUM=page_num,
    )

    if not html_content:
        logger.info(
            "No content retrieved, indicating end of pages.",
            page=page_num,
            job_category_code=job_category_code,
        )
        return
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    job_urls_on_page = _parse_job_urls(soup, page_num)
    if not job_urls_on_page:
        logger.info("No job URLs found on page, indicating end of pages.", page=page_num, job_category_code=job_category_code)
        return

    logger.info("page_found_urls", count=len(job_urls_on_page), page=page_num, category=job_category_code)
    for url in job_urls_on_page:
        processed_url = url
        # Apply the transformation logic
        if "www.cake.me/jobs/" in url:
            transformed_url = url.replace("https://www.cake.me/jobs/", "https://www.cake.me/companies/")
            if transformed_url != url:
                processed_url = transformed_url
                logger.info("Transformed URL for processing.", original_url=url, new_url=processed_url)
        _process_single_job_url(processed_url, job_category_code, db_name)

    # For CakeResume, we don't have a direct max_page from the initial response.
    # We rely on the absence of new URLs or content to stop.
    # For testing, max_page can be passed.
    if max_page is None or page_num < max_page:
        next_page_num = page_num + 1
        logger.info("chaining_next_page_task", next_page=next_page_num, max_page=max_page, category=job_category_code)
        task_crawl_cakeresume_page_and_chain(job_category_code=job_category_code, page_num=next_page_num, max_page=max_page, db_name=db_name)
    else:
        logger.info("reached_max_page", page=page_num, max_page=max_page, category=job_category_code)

@app.task
def task_start_cakeresume_crawl_chain(job_category: dict, db_name: Optional[str] = None, max_page: Optional[int] = None):
    try:
        category = CategorySourcePydantic.model_validate(job_category)
        job_category_code = category.source_category_id
    except Exception as e:
        logger.error("invalid_job_category_data", data=job_category, error=str(e), exc_info=True)
        return
    logger.info("start_task_chain", job_category_code=job_category_code)
    task_crawl_cakeresume_page_and_chain(job_category_code=job_category_code, page_num=1, max_page=max_page, db_name=db_name)

# --- Local Test Runner ---
def _run_local_test():
    db_name_for_local_run = os.environ.get('CRAWLER_DB_NAME') or get_db_name_for_platform(SourcePlatform.PLATFORM_CAKERESUME.value)
    initialize_database(db_name=db_name_for_local_run)

    n_days = 7
    
    all_categories_pydantic: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_CAKERESUME, db_name=db_name_for_local_run)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories_pydantic}
    all_crawled_category_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_CAKERESUME, db_name=db_name_for_local_run)
    stale_crawled_category_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_CAKERESUME, n_days, db_name=db_name_for_local_run)
    categories_to_dispatch_ids = (all_category_ids - all_crawled_category_ids) | stale_crawled_category_ids
    categories_to_dispatch = [
        cat for cat in all_categories_pydantic
        if cat.source_category_id in categories_to_dispatch_ids
    ]
    # Sort to prioritize 'it' categories first, then by source_category_id
    categories_to_dispatch.sort(key=lambda x: (not x.parent_source_id or not x.parent_source_id.startswith('it'), x.source_category_id))

    if categories_to_dispatch:
        logger.info(f"Found {len(categories_to_dispatch)} categories to dispatch for testing.")
        for job_category in categories_to_dispatch:
            logger.info(
                "Dispatching task_start_cakeresume_crawl_chain for local testing.",
                job_category_code=job_category.source_category_id,
            )
            task_start_cakeresume_crawl_chain(job_category.model_dump(), db_name=db_name_for_local_run, max_page=10) # Added max_page for testing
    else:
        logger.warning("No valid and dispatchable categories found for testing. Please check the database for valid categories.")
    logger.info("All categories processed for local testing.")

if __name__ == "__main__":
    _run_local_test()
