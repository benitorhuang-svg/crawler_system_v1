import os

# #  python -m crawler.project_104.task_urls_104
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import structlog
import time
from typing import Optional, List, Dict, Any, Set, Tuple
import requests
import functools
from collections import defaultdict
import json

from crawler.worker import app
from crawler.database.schemas import SourcePlatform, JobPydantic, UrlPydantic, CategorySourcePydantic, JobObservationPydantic
from crawler.database.repository import upsert_jobs, upsert_urls, upsert_url_categories, get_all_categories_for_platform, insert_job_observations
from crawler.project_104.client_104 import fetch_job_urls_from_104_api
from crawler.project_104.parser_apidata_104 import parse_job_item_to_pydantic
from crawler.database.connection import initialize_database
from crawler.config import get_db_name_for_platform, URL_CRAWLER_UPLOAD_BATCH_SIZE, URL_CRAWLER_REQUEST_TIMEOUT_SECONDS, MYSQL_DATABASE, URL_CRAWLER_API_RETRIES, URL_CRAWLER_API_BACKOFF_FACTOR
from crawler.project_104.config_104 import URL_CRAWLER_BASE_URL_104, URL_CRAWLER_PAGE_SIZE_104, HEADERS_104_URL_CRAWLER, URL_CRAWLER_ORDER_BY_104

logger = structlog.get_logger(__name__)

def _fetch_job_list_page(session: requests.Session, base_params: Dict[str, Any], page_num: int, retries: int = URL_CRAWLER_API_RETRIES, backoff_factor: float = URL_CRAWLER_API_BACKOFF_FACTOR, verify_ssl: bool = True) -> Optional[Dict[str, Any]]:
    """
    Fetches a single job list page from the 104 API and returns its JSON response.
    Includes retry logic with exponential backoff.
    """
    params = base_params.copy()
    params['page'] = page_num

    for attempt in range(retries):
        try:
            api_response = fetch_job_urls_from_104_api(
                URL_CRAWLER_BASE_URL_104,
                HEADERS_104_URL_CRAWLER,
                params,
                URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
                verify=verify_ssl,
                session=session
            )
            return api_response
        except requests.exceptions.RequestException as e:
            logger.warning(
                "API request failed, retrying...",
                attempt=attempt + 1,
                max_attempts=retries,
                error=str(e),
                page=page_num
            )
            time.sleep(backoff_factor * (2 ** attempt))
    logger.error("API request failed after multiple retries.", page=page_num)
    return None


def _upsert_batch_data(jobs_for_upsert: List[JobPydantic], jobs_for_observations: List[JobPydantic], category_tags: List[Dict[str, str]], db_name: str):
    """
    Helper function to upsert collected data to the database.
    """
    if jobs_for_upsert:
        urls_to_upsert = [
            UrlPydantic(source_url=job.url, source=job.source_platform)
            for job in jobs_for_upsert
        ]
        upsert_jobs(jobs_for_upsert, db_name=db_name)
        upsert_urls(SourcePlatform.PLATFORM_104, urls_to_upsert, db_name=db_name)
        upsert_url_categories(category_tags, db_name=db_name)
        
        # Insert into tb_job_observations
        job_observations = []
        for job in jobs_for_observations:
            job_observations.append(JobObservationPydantic(
                source_job_id=job.source_job_id,
                source_platform=job.source_platform,
                url=job.url,
                title=job.title,
                description=job.description,
                job_type=job.job_type,
                posted_at=job.posted_at,
                status=job.status,
                salary_text=job.salary_text,
                salary_min=job.salary_min,
                salary_max=job.salary_max,
                salary_type=job.salary_type,
                experience_required_text=job.experience_required_text,
                education_required_text=job.education_required_text,
                company_id=job.company.source_company_id if job.company else None,
                company_name=job.company.name if job.company else None,
                company_url=job.company.url if job.company else None,
                location_text=job.locations[0].address_detail if job.locations and job.locations and job.locations[0].address_detail else None,
                region=job.locations[0].region if job.locations else None,
                district=job.locations[0].district if job.locations else None,
                latitude=job.locations[0].latitude if job.locations else None,
                longitude=job.locations[0].longitude if job.locations else None,
                skills=", ".join([skill.name for skill in job.skills]) if job.skills else None,
            ))
        insert_job_observations(job_observations, db_name=db_name)

        logger.info("Batch data upserted to database.", jobs_upserted=len(jobs_for_upsert), urls_count=len(urls_to_upsert), category_tags_count=len(category_tags), observations_count=len(job_observations))
    else:
        logger.info("No data to upsert in this batch.", db_name=db_name)


def _get_db_name(db_name_override: Optional[str]) -> str:
    """
    Determines the database name to use based on override or environment variables.
    """
    if db_name_override:
        return db_name_override
    elif os.environ.get('CRAWLER_DB_NAME'):
        return MYSQL_DATABASE
    else:
        return get_db_name_for_platform(SourcePlatform.PLATFORM_104.value)


def _process_job_items(api_job_urls: List[Dict[str, Any]], job_url_set_local: set, global_job_url_set: set) -> Tuple[List[JobPydantic], List[JobPydantic], List[Dict[str, str]]]:
    """
    Processes a list of raw job items, parses them, and prepares them for upsertion and observation.
    Returns (jobs_for_upsert, jobs_for_observations, job_category_tags_to_upsert).
    """
    jobs_for_upsert: List[JobPydantic] = []
    jobs_for_observations: List[JobPydantic] = []
    job_category_tags_to_upsert: List[Dict[str, str]] = []

    for job_item_raw in api_job_urls:
        job_pydantic = parse_job_item_to_pydantic(job_item_raw)
        if job_pydantic:
            # Always add to observations list
            jobs_for_observations.append(job_pydantic)

            # Add to upsert list only if unique within this category crawl
            if job_pydantic.source_job_id not in job_url_set_local:
                job_url_set_local.add(job_pydantic.source_job_id)
                global_job_url_set.add(job_pydantic.source_job_id)
                jobs_for_upsert.append(job_pydantic)
            else:
                logger.debug("Skipping duplicate job ID for upsert (already seen in this category crawl).", job_id=job_pydantic.source_job_id)

            # Add category tags for all jobs (observations)
            if job_pydantic.source_job_id and job_pydantic.category_tags:
                for cat_id in job_pydantic.category_tags:
                    job_category_tags_to_upsert.append(
                        {
                            "job_id": job_pydantic.source_job_id,
                            "category_source_id": cat_id,
                        }
                    )
        else:
            logger.warning("Failed to parse job item to Pydantic model.", job_item_raw=job_item_raw)

    return jobs_for_upsert, jobs_for_observations, job_category_tags_to_upsert


def _crawl_category_pages(job_category_code: str, url_limit: int, db_name: str, global_job_url_set: Set[str], verify_ssl: bool = True) -> Set[str]:
    """
    Core crawling logic for a single job category, iterating through pages.
    """
    jobs_for_upsert: List[JobPydantic] = []
    jobs_for_observations: List[JobPydantic] = []
    job_category_tags_for_all_jobs: List[Dict[str, str]] = []

    page = 1
    max_page = 1 # Initial value for max_page, will be updated from API
    job_url_set_local = set() # Use a local set for this category's URLs

    base_params = {
        'jobsource': 'm_joblist_search',
        'pagesize': URL_CRAWLER_PAGE_SIZE_104,
        'order': URL_CRAWLER_ORDER_BY_104,
        'jobcat': job_category_code,
    }

    with requests.Session() as session:
        partial_fetch_job_list_page = functools.partial(
            _fetch_job_list_page,
            session=session,
            base_params=base_params,
            verify_ssl=verify_ssl
        )
        while page <= max_page:
            logger.info("Fetching job list page", current_page=page, max_page=max_page, category=job_category_code)
            api_response = None
            try:
                api_response = partial_fetch_job_list_page(page_num=page)
                
                api_job_urls = api_response.get('data', []) if api_response else []
                pagination_data = api_response.get("metadata", {}).get("pagination") if api_response else None
                # logger.info("API Response pagination data", pagination=pagination_data, category=job_category_code)
                
                # Update max_page from API response if available
                if pagination_data and "lastPage" in pagination_data:
                    max_page = pagination_data["lastPage"]
                    logger.info("Updated max_page", new_max_page=max_page, category=job_category_code)

            except Exception:
                logger.error("Unexpected error during API request for page={page}: {e}. Skipping this page.", exc_info=True, page=page)
                page += 1 # Try next page on unexpected error
                continue

            if not api_job_urls and page >= max_page:
                logger.info("No job items found on page, stopping crawling for this category.", page=page, category=job_category_code)
                break

            current_page_jobs_for_upsert, current_page_jobs_for_observations, current_page_job_category_tags = _process_job_items(api_job_urls, job_url_set_local, global_job_url_set)
            jobs_for_upsert.extend(current_page_jobs_for_upsert)
            jobs_for_observations.extend(current_page_jobs_for_observations)
            job_category_tags_for_all_jobs.extend(current_page_job_category_tags)

            # Check if batch size reached for upload
            if len(jobs_for_upsert) >= URL_CRAWLER_UPLOAD_BATCH_SIZE:
                logger.info("Batch upload size reached. Starting data upload.", count=len(jobs_for_upsert), category=job_category_code)
                # Pass jobs_for_upsert for upsert, and jobs_for_observations for observations
                _upsert_batch_data(jobs_for_upsert, jobs_for_observations, job_category_tags_for_all_jobs, db_name) # urls are handled by upsert_jobs
                
                jobs_for_upsert.clear()
                jobs_for_observations.clear()
                job_category_tags_for_all_jobs.clear()

            page += 1

            # Apply url_limit if it's set and we've exceeded it
            if url_limit > 0 and len(global_job_url_set) >= url_limit:
                logger.info("URL limit reached, stopping crawling.", url_limit=url_limit, category=job_category_code)
                break

    # Store any remaining items in the batch
    _upsert_batch_data(jobs_for_upsert, jobs_for_observations, job_category_tags_for_all_jobs, db_name) # urls are handled by upsert_jobs

    return global_job_url_set


@app.task()
def crawl_and_store_category_urls(job_category: dict, url_limit: int = 0, db_name_override: Optional[str] = None, global_job_url_set: Set[str] = None, verify_ssl: bool = True) -> int:
    """
    Celery task: Iterates through all pages of a specified 104 job category, fetches job details,
    and stores them in the database in batches using concurrent fetching.
    """
    job_category_pydantic = CategorySourcePydantic.model_validate(job_category)
    job_category_code = job_category_pydantic.source_category_id
    db_name = _get_db_name(db_name_override)
    
    logger.info(
        "Task started: crawling 104 jobs for category.",
        job_category_code=job_category_code,
        url_limit=url_limit,
        platform=SourcePlatform.PLATFORM_104,
    )

    total_collected_job_ids = _crawl_category_pages(job_category_code, url_limit, db_name, global_job_url_set, verify_ssl)

    logger.info("Task execution finished.", job_category_code=job_category_code, total_collected=len(total_collected_job_ids))
    return total_collected_job_ids


if __name__ == "__main__":
    # Determine the actual database name for local testing
    actual_db_name_for_local_test = _get_db_name(None)

    logger.info(f"Local test mode: Using database '{actual_db_name_for_local_test}'")

    initialize_database(db_name=actual_db_name_for_local_test)
    logger.info(f"Database '{actual_db_name_for_local_test}' initialized for local test.")

    all_categories = get_all_categories_for_platform(SourcePlatform.PLATFORM_104, db_name=actual_db_name_for_local_test)
    
    # Add a global set to track all unique job IDs collected across all categories
    all_collected_job_ids = set()

    if all_categories:
        # Identify all parent category IDs
        parent_category_ids = {cat.parent_source_id for cat in all_categories if cat.parent_source_id is not None}

        # Filter out categories that are parents themselves
        # Keep only categories whose source_category_id is NOT in the set of parent_category_ids
        filtered_categories = [cat for cat in all_categories if cat.source_category_id not in parent_category_ids]

        # Load PARENT_ORDER_LIST from major_categories.json
        major_categories_path = "/home/soldier/crawler_system_v0_local_test/crawler/utils/skill_data/source_data/major_categories.json"
        try:
            with open(major_categories_path, 'r', encoding='utf-8') as f:
                major_categories_data = json.load(f)
            PARENT_ORDER_LIST = [item['source_category_id'] for item in major_categories_data]
        except FileNotFoundError:
            logger.error(f"major_categories.json not found at {major_categories_path}. Using default order.")
            PARENT_ORDER_LIST = [] # Fallback to empty list or a default hardcoded list if needed
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {major_categories_path}. Using default order.")
            PARENT_ORDER_LIST = []

        # Group categories by parent_source_id
        categories_by_parent = defaultdict(list)
        for category in filtered_categories: # Use filtered_categories here
            categories_by_parent[category.parent_source_id].append(category)

        # Sort categories based on PARENT_ORDER_LIST
        sorted_categories = []
        for parent_id in PARENT_ORDER_LIST:
            if parent_id in categories_by_parent:
                # Sort categories within each parent group by source_category_name
                sorted_categories.extend(sorted(categories_by_parent[parent_id], key=lambda x: x.source_category_name))
                del categories_by_parent[parent_id]
        
        # Add any remaining categories (those not in PARENT_ORDER_LIST)
        for parent_id in sorted(categories_by_parent.keys()):
            sorted_categories.extend(sorted(categories_by_parent[parent_id], key=lambda x: x.source_category_name))

        # Iterate through all categories for local testing
        for category in sorted_categories:
            logger.info("Dispatching crawl_and_store_category_urls task for local testing.", category=category.source_category_name)
            collected_job_ids_for_category = crawl_and_store_category_urls(category.model_dump(), url_limit=0, db_name_override=actual_db_name_for_local_test, global_job_url_set=all_collected_job_ids, verify_ssl=False)
            all_collected_job_ids.update(collected_job_ids_for_category)
            logger.info("Finished crawling category.", category=category.source_category_name, collected_count=len(collected_job_ids_for_category), total_unique_so_far=len(all_collected_job_ids))
            
        logger.info("All categories processed. Total unique job IDs collected across all categories.", total_unique_jobs=len(all_collected_job_ids))

    else:
        logger.warning("No categories found in database for testing. Please run producer_category_104 first.")
