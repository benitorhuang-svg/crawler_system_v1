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
from crawler.database.schemas import CrawlStatus
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled
from crawler.project_yourator.client_yourator import fetch_job_data_from_yourator_api
from crawler.project_yourator.parser_apidata_yourator import parse_job_detail_to_pydantic

logger = structlog.get_logger(__name__)


@app.task()
def fetch_url_data_yourator(url: str) -> Optional[dict]:
    job_id = None
    try:
        job_id = url.split("/")[-1]
        if not job_id:
            logger.error("Failed to extract job_id from URL.", url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        data = fetch_job_data_from_yourator_api(job_id)
        if data is None:
            logger.error(
                "Failed to fetch job data from Yourator API.", job_id=job_id, url=url
            )
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

    except Exception as e:
        logger.error(
            "Unexpected error during API call or job ID extraction.",
            error=e,
            job_id=job_id,
            url=url,
            exc_info=True,
        )
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return None

    job_pydantic_data = parse_job_detail_to_pydantic(data)

    if not job_pydantic_data:
        logger.error("Failed to parse job data.", job_id=job_id, url=url)
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return None

    try:
        upsert_jobs([job_pydantic_data])
        logger.info("Job parsed and upserted successfully.", job_id=job_id, url=url)
        mark_urls_as_crawled({CrawlStatus.SUCCESS: [url]})
        return job_pydantic_data.model_dump()

    except Exception as e:
        logger.error(
            "Unexpected error when upserting job data.",
            error=e,
            job_id=job_id,
            url=url,
            exc_info=True,
        )
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return None




if __name__ == "__main__":
    initialize_database()

    PRODUCER_BATCH_SIZE = 20000000 # Changed from 10 to 20
    statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING, CrawlStatus.QUEUED]

    logger.info("Fetching URLs to process for local testing.", statuses=statuses_to_fetch, limit=PRODUCER_BATCH_SIZE)

    urls_to_process = get_urls_by_crawl_status(
        platform=SourcePlatform.PLATFORM_YOURATOR,
        statuses=statuses_to_fetch,
        limit=PRODUCER_BATCH_SIZE,
    )

    if urls_to_process:
        logger.info("Found URLs to process.", count=len(urls_to_process))
        for url in urls_to_process:
            logger.info("Processing URL.", url=url)
            fetch_url_data_yourator(url)
    else:
        logger.info("No URLs found to process for testing.")