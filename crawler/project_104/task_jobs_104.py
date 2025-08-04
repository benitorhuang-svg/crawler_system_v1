# import os
# # python -m crawler.project_104.task_jobs_104  
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---


import os
import structlog
from typing import Optional
from crawler.worker import app
from crawler.database.schemas import CrawlStatus, SourcePlatform
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled, get_urls_by_crawl_status
from crawler.project_104.client_104 import fetch_job_data_from_104_api
from crawler.project_104.parser_apidata_104 import parse_job_item_to_pydantic
from crawler.database.connection import initialize_database
from crawler.config import get_db_name_for_platform, MYSQL_DATABASE

logger = structlog.get_logger(__name__)


@app.task()
def fetch_url_data_104(url: str, db_name_override: Optional[str] = None) -> Optional[dict]:
    """
    Celery task: Fetches detailed information for a single job vacancy from a given URL,
    parses it, stores it in the database, and marks the URL's crawl status.
    """
    db_name = db_name_override if db_name_override else get_db_name_for_platform(SourcePlatform.PLATFORM_104.value)
    job_id = None
    try:
        job_id = url.split("/")[-1].split("?")[0]
        if not job_id:
            logger.error("Failed to extract job_id from URL.", url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
            return None

        data = fetch_job_data_from_104_api(job_id)
        if data is None:
            logger.error("Failed to fetch job data from 104 API.", job_id=job_id, url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
            return None

    except Exception as e:
        logger.error(
            "Unexpected error during API call or job ID extraction.",
            error=e,
            job_id=job_id,
            url=url,
            exc_info=True,
        )
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
        return None

    job_api_data = data.get("data")
    if not job_api_data or job_api_data.get("switch") == "off":
        logger.warning("Job content does not exist or is closed.", job_id=job_id, url=url)
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
        return None

    job_pydantic_data = parse_job_item_to_pydantic(job_api_data)

    if not job_pydantic_data:
        logger.error("Failed to parse job data.", job_id=job_id, url=url)
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
        return None

    try:
        upsert_jobs([job_pydantic_data], db_name=db_name)
        logger.info("Job parsed and upserted successfully.", job_id=job_id, url=url)
        mark_urls_as_crawled({CrawlStatus.SUCCESS: [url]}, db_name=db_name)
        return job_pydantic_data.model_dump()

    except Exception as e:
        logger.error(
            "Unexpected error when upserting job data.",
            error=e,
            job_id=job_id,
            url=url,
            exc_info=True,
        )
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]}, db_name=db_name)
        return None


if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
    initialize_database()

    PRODUCER_BATCH_SIZE = 20000000 # Changed from 10 to 20
    statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING, CrawlStatus.QUEUED]
    
    logger.info("Fetching URLs to process for local testing.", statuses=statuses_to_fetch, limit=PRODUCER_BATCH_SIZE)

    urls_to_process = get_urls_by_crawl_status(
        platform=SourcePlatform.PLATFORM_104,
        statuses=statuses_to_fetch,
        limit=PRODUCER_BATCH_SIZE,
        db_name=None
    )

    if urls_to_process:
        logger.info("Found URLs to process.", count=len(urls_to_process))
        for url in urls_to_process:
            logger.info("Processing URL.", url=url)
            fetch_url_data_104(url, db_name_override=None)
    else:
        logger.info("No URLs found to process for testing.")