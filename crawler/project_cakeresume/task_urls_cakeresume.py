# import os
# # python -m crawler.project_cakeresume.task_urls_cakeresume
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import json
import structlog
from collections import deque
from bs4 import BeautifulSoup
from typing import Set, List, Dict

from crawler.worker import app
from crawler.database.schemas import SourcePlatform, UrlCategoryPydantic, CategorySourcePydantic
from crawler.database.repository import (
    upsert_urls,
    upsert_url_categories,
    get_all_categories_for_platform,
    get_all_crawled_category_ids_pandas,
    get_stale_crawled_category_ids_pandas,
)
from crawler.project_cakeresume.client_cakeresume import fetch_cakeresume_job_urls
from crawler.database.connection import initialize_database
from crawler.config import (
    URL_CRAWLER_UPLOAD_BATCH_SIZE,
    get_db_name_for_platform,
)
from crawler.project_cakeresume.config_cakeresume import (
    URL_CRAWLER_ORDER_BY_CAKERESUME,
    JOB_DETAIL_BASE_URL_CAKERESUME,
)

logger = structlog.get_logger(__name__)


@app.task
def crawl_and_store_cakeresume_category_urls(job_category: dict, url_limit: int = 0) -> None:
    """
    Celery task: 迭代指定 CakeResume 工作分類的所有頁面，獲取職缺 URL，並將其存儲到資料庫。
    """
    job_category_pydantic = CategorySourcePydantic.model_validate(job_category)
    db_name = get_db_name_for_platform(job_category_pydantic.source_platform.value)
    try:
        crawler = CakeResumeCrawler(job_category_pydantic, url_limit, db_name)
        crawler.run()
    except Exception as e:
        logger.error(
            "An unexpected error occurred during the crawling task.",
            job_category=job_category,
            error=str(e),
            exc_info=True
        )

class CakeResumeCrawler:
    """
    將爬蟲的狀態和邏輯封裝在此類中，以提高程式碼的可讀性和可維護性。
    """
    def __init__(self, job_category: CategorySourcePydantic, url_limit: int = 0, db_name: str = None):
        self.job_category = job_category
        self.job_category_code = self.job_category.source_category_id
        self.url_limit = url_limit
        self.db_name = db_name
        self.global_job_url_set: Set[str] = set()
        self.current_batch_urls: List[str] = []
        self.current_batch_url_categories: List[Dict] = []
        self.recent_counts = deque(maxlen=4)
        self.current_page = 1

        logger.info(
            "Crawler initialized for CakeResume job category.",
            job_category_code=self.job_category_code,
            url_limit=self.url_limit
        )

    def run(self) -> None:
        """
        執行爬蟲任務的主函式。
        """
        while True:
            if 0 < self.url_limit <= len(self.global_job_url_set):
                logger.info(
                    "URL limit reached. Ending task early.",
                    collected_urls=len(self.global_job_url_set)
                )
                break
            
            if self.current_page % 5 == 0 or self.current_page == 1:
                 logger.info(
                    "Current page being processed.",
                    page=self.current_page,
                    job_category_code=self.job_category_code,
                )

            html_content = fetch_cakeresume_job_urls(
                KEYWORDS="",
                CATEGORY=self.job_category_code,
                ORDER=URL_CRAWLER_ORDER_BY_CAKERESUME,
                PAGE_NUM=self.current_page,
            )

            if not html_content:
                logger.info(
                    "No content retrieved, indicating end of pages.",
                    page=self.current_page,
                    job_category_code=self.job_category_code,
                )
                break
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            job_urls_on_page = self._parse_job_urls(soup)
            if not job_urls_on_page:
                logger.info("No job URLs found on page, indicating end of pages.", page=self.current_page, job_category_code=self.job_category_code)
                break

            new_urls_found = self._process_urls(job_urls_on_page)
            if not new_urls_found and len(self.global_job_url_set) > 0:
                logger.info("No new unique URLs found on this page. Ending task.", page=self.current_page, job_category_code=self.job_category_code)
                break

            if self._check_for_stagnation():
                break

            self.current_page += 1

        self._flush_batch_to_db()
        logger.info("Crawling task finished for job category.", job_category_code=self.job_category_code)

    def _parse_job_urls(self, soup: BeautifulSoup) -> List[str]:
        """從 HTML 中解析出職缺的 URL 列表。"""
        urls = []
        
        next_data_script = soup.find('script', id='__NEXT_DATA__')
        if next_data_script:
            try:
                data = json.loads(next_data_script.string)
                results = data.get('props', {}).get('pageProps', {}).get('serverState', {}).get('initialResults', {}).get('Job', {}).get('results', [{}])[0].get('hits', [])
                for job in results:
                    if 'path' in job and 'page' in job and 'path' in job['page']:
                        full_url = f"{JOB_DETAIL_BASE_URL_CAKERESUME}/companies/{job['page']['path']}/jobs/{job['path']}"
                        urls.append(full_url)
                if urls:
                    return urls
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning("Could not parse __NEXT_DATA__ JSON or key not found.", error=str(e))

        job_links = soup.find_all('a', class_='JobSearchItem_jobTitle__bu6yO')
        for link in job_links:
            href = link.get('href')
            if href:
                if href.startswith('http'):
                    full_url = href
                else:
                    full_url = f"{JOB_DETAIL_BASE_URL_CAKERESUME}{href}"
                urls.append(full_url)
        return urls
    
    def _process_urls(self, urls: List[str]) -> bool:
        """處理新抓取的 URL，並在需要時將其寫入資料庫。"""
        new_urls_added = False
        for original_url in urls:
            processed_url = original_url

            # Apply the transformation logic
            if "www.cake.me/jobs/" in original_url:
                transformed_url = original_url.replace("https://www.cake.me/jobs/", "https://www.cake.me/companies/")
                if transformed_url != original_url:
                    processed_url = transformed_url
                    logger.info("Transformed URL for processing.", original_url=original_url, new_url=processed_url)

            if processed_url not in self.global_job_url_set:
                new_urls_added = True
                self.global_job_url_set.add(processed_url)
                self.current_batch_urls.append(processed_url)
            
            self.current_batch_url_categories.append(
                UrlCategoryPydantic(
                    source_url=processed_url,
                    source_category_id=self.job_category_code,
                ).model_dump()
            )

        if len(self.current_batch_urls) >= URL_CRAWLER_UPLOAD_BATCH_SIZE:
            self._flush_batch_to_db()
            
        return new_urls_added

    def _flush_batch_to_db(self) -> None:
        """將累積的批次資料寫入資料庫。"""
        if not self.current_batch_urls:
            return

        logger.info(
            "Storing batch of URLs and URL-category relations to database.",
            url_count=len(self.current_batch_urls),
            category_relation_count=len(self.current_batch_url_categories),
        )
        upsert_urls(SourcePlatform.PLATFORM_CAKERESUME, self.current_batch_urls, db_name=self.db_name)
        upsert_url_categories(self.current_batch_url_categories, db_name=self.db_name)
        self.current_batch_urls.clear()
        self.current_batch_url_categories.clear()

    def _check_for_stagnation(self) -> bool:
        """檢查是否連續多頁沒有抓到新的職缺。"""
        total_jobs = len(self.global_job_url_set)
        self.recent_counts.append(total_jobs)
        if len(self.recent_counts) == self.recent_counts.maxlen and len(set(self.recent_counts)) == 1 and total_jobs > 0:
            logger.info(
                "No new data found for the last few pages. Ending task early.",
                max_len=self.recent_counts.maxlen
            )
            return True
        return False


if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
    initialize_database()

    n_days = 7
    url_limit = 1000000

    all_categories_pydantic: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_CAKERESUME, db_name=None)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories_pydantic}
    all_crawled_category_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_CAKERESUME, db_name=None)
    stale_crawled_category_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_CAKERESUME, n_days, db_name=None)
    categories_to_dispatch_ids = (all_category_ids - all_crawled_category_ids) | stale_crawled_category_ids
    categories_to_dispatch = [
        cat for cat in all_categories_pydantic 
        if cat.source_category_id in categories_to_dispatch_ids
    ]
    categories_to_dispatch.sort(key=lambda x: x.source_category_id)

    if categories_to_dispatch:
        for job_category in categories_to_dispatch:
            logger.info(
                "Dispatching crawl_and_store_cakeresume_category_urls task for local testing.",
                job_category_code=job_category.source_category_id,
                url_limit=url_limit,
            )
            crawl_and_store_cakeresume_category_urls(job_category.model_dump(), url_limit=url_limit)
    else:
        logger.info("No categories found to dispatch for testing.")