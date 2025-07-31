import os
import structlog
from typing import Optional
import re
import json
from bs4 import BeautifulSoup

# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---

from crawler.worker import app
from crawler.database.schemas import CrawlStatus, SourcePlatform
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled, get_urls_by_crawl_status
from crawler.project_cakeresume.client_cakeresume import fetch_cakeresume_job_data
from crawler.project_cakeresume.parser_apidata_cakeresume import parse_cakeresume_job_data_to_pydantic
from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)


@app.task()
def fetch_url_data_cakeresume(url: str) -> Optional[dict]:
    job_id = None
    try:
        job_id_match = re.search(r'/jobs/([a-zA-Z0-9]+)', url)
        job_id = job_id_match.group(1) if job_id_match else None

        if not job_id:
            logger.error("Failed to extract job_id from URL.", url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        html_content = fetch_cakeresume_job_data(url)
        if html_content is None:
            logger.error("Failed to fetch job data from CakeResume web.", job_id=job_id, url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        soup = BeautifulSoup(html_content, 'html.parser')
        data_script = soup.find('script', id='__NEXT_DATA__')

        if not data_script:
            logger.error("Error: Could not find job data (script#__NEXT_DATA__) on the page.", url=url, job_id=job_id)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        page_props = json.loads(data_script.string)['props']['pageProps']
        job_details = page_props.get('job')

        if not job_details:
            logger.error("Error: Could not parse job data ('job' key not found) from JSON.", url=url, job_id=job_id)
            mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
            return None

        job_pydantic_data = parse_cakeresume_job_data_to_pydantic(job_details, url)

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
            "Unexpected error when processing CakeResume job data.",
            error=e,
            job_id=job_id if 'job_id' in locals() else "N/A",
            url=url,
            exc_info=True,
        )
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return None


if __name__ == "__main__":
    # To run this script for local testing, execute:
    # python -m crawler.project_cakeresume.task_jobs_cakeresume
    # This will automatically use the 'test_db' as configured at the top of the script.

    from crawler.database.connection import initialize_database
    initialize_database()

    statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING, CrawlStatus.QUEUED]
    urls_to_process = get_urls_by_crawl_status(
        platform=SourcePlatform.PLATFORM_CAKERESUME,
        statuses=statuses_to_fetch,
        limit=10,
    )
    for url in urls_to_process:
        logger.info("Starting to process URL from the database.", url=url)
        fetch_url_data_cakeresume(url)
