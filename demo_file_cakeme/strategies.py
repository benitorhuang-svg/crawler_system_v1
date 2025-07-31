import logging
from typing import Dict, Any, Generator, Optional
import json

from bs4 import BeautifulSoup
from crawler.database.schema import Job
from crawler.utils import make_request
from crawler.database import repository # 新增：導入 repository
from crawler.enums import SourcePlatform # 新增：導入 SourcePlatform
from . import parsers

logger = logging.getLogger(__name__)

class HtmlUrlFetcher:
    """
    Strategy Implementation: Fetches job URLs by scraping the
    category-specific HTML pages, as the search API is not available.
    """
    def __init__(self, settings: Any):
        # 移除 categories 參數，UrlFetcher 將在內部獲取分類
        self.cfg = settings
        self.base_url = "https://www.cakeresume.com"
        self.platform = SourcePlatform.PLATFORM_CAKERESUME # 設置平台枚舉

    def __call__(self) -> Generator[Dict[str, Any], None, None]:
        logger.info(f"[{self.platform.value}] 開始從資料庫獲取分類。")
        # 從資料庫獲取該平台的所有分類
        # 我們只需要有 parent_source_id 的子分類
        categories = [cat for cat in repository.get_categories_by_platform(self.platform) if cat.parent_source_id]
        
        if not categories:
            logger.warning(f"[{self.platform.value}] UrlFetcher 未從資料庫獲取到任何分類，將跳過 URL 抓取。")
            return

        logger.info(f"[{self.platform.value}] Starting to fetch from HTML pages for {len(categories)} categories.")
        
        for category in categories:
            category_id = category.source_category_id
            target_url = f"{self.base_url}/jobs/categories/{category_id}"
            
            page = 1
            while True: # 無限循環，直到沒有更多職缺或達到 max_pages 限制
                if self.cfg.max_pages is not None and page > self.cfg.max_pages:
                    logger.info(f"[{self.platform.value}] 已達到分類 {category_id} 的最大頁數限制 ({self.cfg.max_pages})。")
                    break

                params = {'page': page}
                logger.debug(f"[{self.platform.value}] Fetching page {page} for category: {category_id}")
                try:
                    res = make_request(
                        target_url,
                        headers=self.cfg.headers,
                        params=params,
                        delay=self.cfg.request_delay # 傳遞延遲參數
                    )
                    
                    soup = BeautifulSoup(res.text, "html.parser")
                    # Use the correct selector for job links
                    job_links = soup.select("a.JobSearchItem_jobTitle__bu6yO")
                    
                    if not job_links:
                        logger.info(f"[{self.platform.value}] No more jobs found for category {category_id} at page {page}.")
                        break
                    
                    for link in job_links:
                        if href := link.get('href'):
                            # The href is a relative path, e.g., /companies/company/jobs/job-id
                            # The orchestrator will handle joining it with the base URL
                            yield {'href': href}
                    
                    page += 1 # 頁數遞增

                except Exception as e:
                    logger.error(f"[{self.platform.value}] Failed to fetch HTML for category {category_id}, page {page}: {e}", exc_info=True)
                    break


class HtmlDetailFetcher:
    """Strategy: Fetches the full HTML of a single job detail page."""
    def __init__(self, settings: Any):
        self.cfg = settings

    def __call__(self, url: str) -> str:
        try:
            res = make_request(url, headers=self.cfg.headers, delay=self.cfg.request_delay) # 傳遞延遲參數
            return res.text
        except Exception as e:
            logger.error(f"[Cakeresume] Failed to fetch detail for url {url}: {e}")
            return ""

class ScriptDetailParser:
    """
    Strategy: Finds the <script type="application/ld+json"> tag in the
    raw HTML content and passes its content to a dedicated parser.
    """
    def __call__(self, raw_content: str, url: str, intermediate_data: Optional[Dict[str, Any]]) -> Optional[Job]:
        soup = BeautifulSoup(raw_content, "html.parser")
        
        # Cakeresume detail pages embed job data in a script tag with id="__NEXT_DATA__"
        script_tag = soup.find("script", id="__NEXT_DATA__")
        
        if not script_tag or not hasattr(script_tag, 'string') or not script_tag.string:
            logger.warning(f"[Cakeresume] Could not find __NEXT_DATA__ script tag or it is empty on page: {url}")
            return None
        
        try:
            next_data = json.loads(script_tag.string)
            # The actual job data is usually nested under props.pageProps.job
            job_data = next_data.get("props", {}).get("pageProps", {}).get("job")
            
            if not job_data:
                logger.warning(f"[Cakeresume] Could not find job data within __NEXT_DATA__ for page: {url}")
                return None
            
            # Pass the extracted job_data (dict) and the original raw_content (HTML) to the parser
            return parsers.transform_script_to_job_model(job_data, raw_content, url) # Pass raw_content here
        except json.JSONDecodeError as e:
            logger.error(f"[Cakeresume] Failed to decode __NEXT_DATA__ JSON for url {url}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"[Cakeresume] An unexpected error occurred while processing __NEXT_DATA__ for url {url}: {e}", exc_info=True)
            return None