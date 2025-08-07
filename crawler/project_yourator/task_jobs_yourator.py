import os
from crawler.database.connection import initialize_database
from crawler.database.repository import get_urls_by_crawl_status
from crawler.database.schemas import SourcePlatform
# # python -m crawler.project_yourator.task_jobs_yourator
# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---


import structlog
from typing import Optional
from crawler.worker import app
from crawler.database.schemas import CrawlStatus, JobObservationPydantic
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled, insert_job_observations
from crawler.project_yourator.client_yourator import fetch_job_data_from_yourator_api
from crawler.project_yourator.parser_apidata_yourator import parse_job_detail_to_pydantic
from crawler.config import get_db_name_for_platform

logger = structlog.get_logger(__name__)


@app.task()
def fetch_url_data_yourator(url: str) -> Optional[dict]:
    db_name = get_db_name_for_platform(SourcePlatform.PLATFORM_YOURATOR.value)
    job_id = None
    try:
        job_id = url.split("/")[-1]
        if not job_id:
            logger.error(
                "Failed to extract job_id from URL.",
                event="job_id_extraction_failed",
                url=url,
                platform=SourcePlatform.PLATFORM_YOURATOR,
                component="task",
            )
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
            return None

        data = fetch_job_data_from_yourator_api(job_id)
        if data is None:
            logger.error(
                "Failed to fetch job data from Yourator API.",
                event="fetch_job_data_failed",
                job_id=job_id,
                url=url,
                platform=SourcePlatform.PLATFORM_YOURATOR,
                component="task",
            )
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
            return None

    except Exception as e:
        logger.error(
            "Unexpected error during API call or job ID extraction.",
            event="unexpected_api_call_error",
            error=str(e),
            job_id=job_id,
            url=url,
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="task",
            exc_info=True,
        )
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
        return None

    job_pydantic_data = parse_job_detail_to_pydantic(data)

    if not job_pydantic_data:
        logger.error(
            "Failed to parse job data.",
            event="job_data_parsing_failed",
            job_id=job_id,
            url=url,
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="task",
        )
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
        return None

    try:
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
            skills="****".join([skill.name for skill in job_pydantic_data.skills]) if job_pydantic_data.skills else None,
        ))
        insert_job_observations(job_observations, db_name=db_name)

        logger.info(
            "Job parsed and upserted successfully.",
            event="job_upsert_success",
            job_id=job_id,
            url=url,
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="task",
        )
        mark_urls_as_crawled({CrawlStatus.SUCCESS: [url]}, db_name=db_name)
        return job_pydantic_data.model_dump()

    except Exception as e:
        logger.error(
            "Unexpected error when upserting job data.",
            event="job_upsert_error",
            error=str(e),
            job_id=job_id,
            url=url,
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="task",
            exc_info=True,
        )
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
        return None




if __name__ == "__main__":
    initialize_database()

    PRODUCER_BATCH_SIZE = 20000000 # Changed from 10 to 20
    statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING, CrawlStatus.QUEUED]

    logger.info(
        "Fetching URLs to process for local testing.",
        event="fetching_urls_for_local_test",
        statuses=statuses_to_fetch,
        limit=PRODUCER_BATCH_SIZE,
        platform=SourcePlatform.PLATFORM_YOURATOR,
        component="task",
    )

    urls_to_process = get_urls_by_crawl_status(
        platform=SourcePlatform.PLATFORM_YOURATOR,
        statuses=statuses_to_fetch,
        limit=PRODUCER_BATCH_SIZE,
    )

    if urls_to_process:
        logger.info(
            "Found URLs to process.",
            event="urls_found_for_processing",
            count=len(urls_to_process),
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="task",
        )
        for url in urls_to_process:
            logger.info(
                "Processing URL.",
                event="processing_url",
                url=url,
                platform=SourcePlatform.PLATFORM_YOURATOR,
                component="task",
            )
            fetch_url_data_yourator(url)
    else:
        logger.info(
            "No URLs found to process for testing.",
            event="no_urls_found_for_testing",
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="task",
        )