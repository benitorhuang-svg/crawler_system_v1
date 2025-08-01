# import os
# # python -m crawler.project_cakeresume.task_urls_cakeresume
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---


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
    get_all_crawled_category_ids_pandas, # Added for 104 template
    get_stale_crawled_category_ids_pandas, # Added for 104 template
)
from crawler.project_cakeresume.client_cakeresume import fetch_cakeresume_job_urls
from crawler.database.connection import initialize_database
from crawler.config import (
    URL_CRAWLER_UPLOAD_BATCH_SIZE,
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
    try:
        crawler = CakeResumeCrawler(job_category, url_limit)
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
    def __init__(self, job_category: Dict, url_limit: int = 0):
        self.job_category = CategorySourcePydantic.model_validate(job_category)
        self.job_category_code = self.job_category.source_category_id
        self.url_limit = url_limit
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
        job_links = soup.find_all('a', class_='JobSearchItem_jobTitle__bu6yO')
        urls = []
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
        for url in urls:
            if url not in self.global_job_url_set:
                new_urls_added = True
                self.global_job_url_set.add(url)
                self.current_batch_urls.append(url)
            
            self.current_batch_url_categories.append(
                UrlCategoryPydantic(
                    source_url=url,
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
            f"Storing batch of {len(self.current_batch_urls)} URLs and "
            f"{len(self.current_batch_url_categories)} URL-category relations to database."
        )
        upsert_urls(SourcePlatform.PLATFORM_CAKERESUME, self.current_batch_urls)
        upsert_url_categories(self.current_batch_url_categories)
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
    initialize_database()

    n_days = 1  # Define n_days for local testing
    url_limit = 1000000 # Set a high limit for full crawling during local testing

    all_categories_pydantic: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_CAKERESUME)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories_pydantic}
    all_crawled_category_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_CAKERESUME)
    stale_crawled_category_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_CAKERESUME, n_days)
    categories_to_dispatch_ids = (all_category_ids - all_crawled_category_ids) | stale_crawled_category_ids
    categories_to_dispatch = [
        cat for cat in all_categories_pydantic 
        if cat.source_category_id in categories_to_dispatch_ids
    ]

    # Only process the first category for local testing
    if categories_to_dispatch:
        # categories_to_process_single = [categories_to_dispatch[0]] # Uncomment to process only the first category
        
        for job_category in categories_to_dispatch:
            logger.info(
                "Dispatching crawl_and_store_cakeresume_category_urls task for local testing.",
                job_category_code=job_category.source_category_id,
                url_limit=url_limit,
            )
            crawl_and_store_cakeresume_category_urls(job_category.model_dump(), url_limit=url_limit)
    else:
        logger.info("No categories found to dispatch for testing.")
