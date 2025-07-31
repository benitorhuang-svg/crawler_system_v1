import os
# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---

import structlog
from typing import Optional
import re
from crawler.worker import app
from crawler.database.schemas import CrawlStatus, SourcePlatform
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled, get_urls_by_crawl_status
from crawler.project_yes123.client_yes123 import fetch_yes123_job_data
from crawler.project_yes123.parser_apidata_yes123 import parse_yes123_job_data_to_pydantic
from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)


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
    # python -m crawler.project_yes123.task_jobs_yes123
    
    # --- Database Initialization for Local Test ---
    from crawler.database.connection import initialize_database
    initialize_database()
    # --- End Database Initialization ---

    statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING, CrawlStatus.QUEUED]
    urls_to_process = get_urls_by_crawl_status(
        platform=SourcePlatform.PLATFORM_YES123,
        statuses=statuses_to_fetch,
        limit=10,
    )
    for url in urls_to_process:
        logger.info("Starting to process URL from the database.", url=url)
        fetch_url_data_yes123(url)