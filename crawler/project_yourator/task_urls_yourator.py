import os
from typing import List, Set, Optional
from collections import deque

from crawler.database.connection import initialize_database
from crawler.database.repository import (
    get_all_categories_for_platform,
    get_all_crawled_category_ids_pandas,
    get_stale_crawled_category_ids_pandas,
    upsert_urls,
    upsert_url_categories,
    upsert_jobs,
    insert_job_observations,
)
from crawler.database.schemas import (
    SourcePlatform,
    UrlCategoryPydantic,
    CategorySourcePydantic,
    JobObservationPydantic,
)
from crawler.project_yourator.client_yourator import fetch_job_urls_from_yourator_api
from crawler.project_yourator.parser_apidata_yourator import parse_job_list_to_pydantic
from crawler.worker import app
from crawler.config import get_db_name_for_platform, URL_CRAWLER_UPLOAD_BATCH_SIZE
import structlog

logger = structlog.get_logger(__name__)

@app.task
def crawl_and_store_yourator_category_urls(job_category: dict, url_limit: int = 0, db_name_override: Optional[str] = None):
    job_category = CategorySourcePydantic.model_validate(job_category)
    job_category_code = job_category.source_category_id
    db_name = db_name_override if db_name_override else get_db_name_for_platform(SourcePlatform.PLATFORM_YOURATOR.value)

    global_job_url_set = set()
    current_batch_jobs = []
    current_batch_urls = []
    current_batch_url_categories = []
    recent_counts = deque(maxlen=4)

    current_page = 1
    logger.info(
        "Task started: crawling job category URLs and data.",
        job_category_code=job_category_code,
        url_limit=url_limit,
    )

    while True:
        if url_limit > 0 and len(global_job_url_set) >= url_limit:
            logger.info(
                "URL limit reached. Ending task early.",
                job_category_code=job_category_code,
                url_limit=url_limit,
                collected_urls=len(global_job_url_set),
            )
            break

        if current_page % 5 == 1:
            logger.info(
                "Current page being processed.",
                page=current_page,
                job_category_code=job_category_code,
            )

        api_response = fetch_job_urls_from_yourator_api(
            page=current_page, category=job_category_code
        )

        if not api_response or "payload" not in api_response:
            logger.warning(
                "Invalid or empty API response.",
                page=current_page,
                category_id=job_category_code,
                response=api_response,
                platform=SourcePlatform.PLATFORM_YOURATOR,
                component="task",
            )
            break

        payload = api_response["payload"]
        jobs = payload.get("jobs", [])

        if not jobs:
            logger.info(
                "No more jobs found for category on this page.",
                page=current_page,
                category_id=job_category_code,
                platform=SourcePlatform.PLATFORM_YOURATOR,
                component="task",
            )
            break

        for job_item in jobs:
            job_pydantic = parse_job_list_to_pydantic(job_item)
            if job_pydantic and job_pydantic.url:
                if job_pydantic.url not in global_job_url_set:
                    global_job_url_set.add(job_pydantic.url)
                    current_batch_jobs.append(job_pydantic)
                    current_batch_urls.append(job_pydantic.url)

                current_batch_url_categories.append(
                    UrlCategoryPydantic(
                        source_url=job_pydantic.url,
                        source_category_id=job_category_code,
                    ).model_dump()
                )

        if len(current_batch_urls) >= URL_CRAWLER_UPLOAD_BATCH_SIZE:
            logger.info(
                "Batch upload size reached. Starting data upload.",
                count=len(current_batch_urls),
                job_category_code=job_category_code,
                platform=SourcePlatform.PLATFORM_YOURATOR,
                component="task",
            )
            upsert_jobs(current_batch_jobs, db_name=db_name)
            upsert_urls(SourcePlatform.PLATFORM_YOURATOR, current_batch_urls, db_name=db_name)
            upsert_url_categories(current_batch_url_categories, db_name=db_name)

            # Insert into tb_job_observations
            job_observations = []
            for job in current_batch_jobs:
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
                    location_text=job.locations[0].address_detail if job.locations else None,
                    region=job.locations[0].region if job.locations else None,
                    district=job.locations[0].district if job.locations else None,
                    latitude=job.locations[0].latitude if job.locations else None,
                    longitude=job.locations[0].longitude if job.locations else None,
                    skills="****".join([skill.name for skill in job.skills]) if job.skills else None,
                ))
            insert_job_observations(job_observations, db_name=db_name)

            current_batch_jobs.clear()
            current_batch_urls.clear()
            current_batch_url_categories.clear()

        recent_counts.append(len(global_job_url_set))
        if len(recent_counts) == recent_counts.maxlen and len(set(recent_counts)) == 1 and len(global_job_url_set) > 0:
            logger.info(
                "No new data found for the last few pages. Ending task early.",
                job_category_code=job_category_code,
            )
            break

        if not payload.get("hasMore", False):
            logger.info(
                "API indicated no more pages for category.",
                category_id=job_category_code,
                page=current_page,
                platform=SourcePlatform.PLATFORM_YOURATOR,
                component="task",
            )
            break

        current_page += 1

    if current_batch_urls:
        logger.info(
            "Task completed. Storing remaining data to database.",
            count=len(current_batch_urls),
            job_category_code=job_category_code,
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="task",
        )
        upsert_jobs(current_batch_jobs, db_name=db_name)
        upsert_urls(SourcePlatform.PLATFORM_YOURATOR, current_batch_urls, db_name=db_name)
        upsert_url_categories(current_batch_url_categories, db_name=db_name)

        # Insert into tb_job_observations
        job_observations = []
        for job in current_batch_jobs:
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
                location_text=job.locations[0].address_detail if job.locations else None,
                region=job.locations[0].region if job.locations else None,
                district=job.locations[0].district if job.locations else None,
                latitude=job.locations[0].latitude if job.locations else None,
                longitude=job.locations[0].longitude if job.locations else None,
                skills="****".join([skill.name for skill in job.skills]) if job.skills else None,
            ))
        insert_job_observations(job_observations, db_name=db_name)
    else:
        logger.info(
            "Task completed. No new data collected, skipping database storage.",
            job_category_code=job_category_code,
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="task",
        )

    logger.info("Task execution finished.",
        job_category_code=job_category_code,
        total_collected=len(global_job_url_set),
        platform=SourcePlatform.PLATFORM_YOURATOR,
        component="task",
    )
    return len(global_job_url_set)


if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
    initialize_database()

    n_days = 7  # Define n_days for local testing
    url_limit = 100000

    all_categories_pydantic: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_YOURATOR, )
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories_pydantic}
    all_crawled_category_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YOURATOR, )
    stale_crawled_category_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_YOURATOR, n_days, )
    categories_to_dispatch_ids = (all_category_ids - all_crawled_category_ids) | stale_crawled_category_ids
    categories_to_dispatch = [
        cat for cat in all_categories_pydantic
        if cat.source_category_id in categories_to_dispatch_ids
    ]
    categories_to_dispatch.sort(key=lambda x: x.source_category_id)

    if categories_to_dispatch:
        logger.info(
            "Found categories to dispatch for local testing.",
            count=len(categories_to_dispatch),
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="task",
        )
        for job_category in categories_to_dispatch:
            logger.info(
                "Dispatching crawl_and_store_yourator_category_urls task for local testing.",
                job_category_code=job_category.source_category_id,
                url_limit=url_limit,
                platform=SourcePlatform.PLATFORM_YOURATOR,
                component="task",
            )
            crawl_and_store_yourator_category_urls(job_category.model_dump(), url_limit=url_limit, db_name_override='test_db')
    else:
        logger.info(
            "No categories found to dispatch for testing.",
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="task",
        )