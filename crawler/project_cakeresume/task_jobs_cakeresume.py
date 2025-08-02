# import os
# # python -m crawler.project_cakeresume.task_jobs_cakeresume
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---


import time
import structlog
import json
from typing import Optional
from bs4 import BeautifulSoup
import re

from crawler.worker import app
from crawler.database.schemas import CrawlStatus, SourcePlatform
from crawler.database.connection import initialize_database
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled, get_urls_by_crawl_status
from crawler.project_cakeresume.client_cakeresume import fetch_cakeresume_job_data
from crawler.project_cakeresume.parser_cakeresume import parse_job_details_to_pydantic

logger = structlog.get_logger(__name__)

@app.task(rate_limit='60/m')
def fetch_url_data_cakeresume(url: str) -> Optional[dict]:
    """
    Celery task: Fetches detailed job info from a URL, parses it, stores it, and marks the URL status.
    """
    original_url = url
    if "www.cake.me/jobs/" in url:
        new_url = url.replace("https://www.cake.me/jobs/", "https://www.cake.me/companies/")
        if new_url != url:
            url = new_url
            logger.info("Transformed URL for processing.", original_url=original_url, new_url=url)

    job_id = None
    try:
        match = re.search(r'/jobs/([a-zA-Z0-9_-]+)', original_url)
        job_id = match.group(1) if match else None

        if not job_id:
            logger.error("Failed to extract job_id from URL.", url=original_url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [original_url]})
            return None

        html_content = fetch_cakeresume_job_data(url)
        if not html_content:
            logger.error("Failed to fetch job data from CakeResume.", job_id=job_id, url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [original_url]})
            return None

        soup = BeautifulSoup(html_content, 'html.parser')
        data_script = soup.find('script', id='__NEXT_DATA__')

        if not data_script:
            logger.error("Could not find __NEXT_DATA__ script tag.", url=url, job_id=job_id)
            mark_urls_as_crawled({CrawlStatus.FAILED: [original_url]})
            return None

        page_props = json.loads(data_script.string).get('props', {}).get('pageProps', {})
        job_details = page_props.get('job')

        if not job_details:
            logger.error("Could not find job details in __NEXT_DATA__.", url=url, job_id=job_id)
            mark_urls_as_crawled({CrawlStatus.FAILED: [original_url]})
            return None
        
        job_pydantic_data = parse_job_details_to_pydantic(job_details, html_content, url)

        if not job_pydantic_data:
            logger.error("Failed to parse job data to Pydantic.", job_id=job_id, url=url)
            mark_urls_as_crawled({CrawlStatus.FAILED: [original_url]})
            return None

        upsert_jobs([job_pydantic_data])
        logger.info("Job parsed and upserted successfully.", job_id=job_id, url=url)
        mark_urls_as_crawled({CrawlStatus.SUCCESS: [original_url]})
        return job_pydantic_data.model_dump()

    except Exception as e:
        logger.error("Unexpected error processing CakeResume job data.", error=e, job_id=job_id, url=original_url, exc_info=True)
        mark_urls_as_crawled({CrawlStatus.FAILED: [original_url]})
        return None


if __name__ == "__main__":
    initialize_database()

    PRODUCER_BATCH_SIZE = 20000000
    statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING, CrawlStatus.QUEUED]
    
    logger.info("Fetching URLs to process for local testing.", statuses=statuses_to_fetch, limit=PRODUCER_BATCH_SIZE)

    urls_to_process = get_urls_by_crawl_status(
        platform=SourcePlatform.PLATFORM_CAKERESUME,
        statuses=statuses_to_fetch,
        limit=PRODUCER_BATCH_SIZE,
    )

    if urls_to_process:
        logger.info("Found URLs to process.", count=len(urls_to_process))
        for url in urls_to_process:
            logger.info("Processing URL.", url=url)
            fetch_url_data_cakeresume(url)
            time.sleep(1)
    else:
        logger.info("No URLs found to process for testing.")