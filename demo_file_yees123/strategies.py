import logging
from typing import Dict, Any, Generator, Optional

from bs4 import BeautifulSoup

from crawler.utils import make_request
from crawler.database.schema import Job
from crawler.database import repository # 新增：導入 repository
from crawler.enums import SourcePlatform # 新增：導入 SourcePlatform
from . import parsers

logger = logging.getLogger(__name__)

class HtmlUrlFetcher:
    def __init__(self, settings: Any):
        # 移除 categories 參數，UrlFetcher 將在內部獲取分類
        self.cfg = settings
        self.platform = SourcePlatform.PLATFORM_YES123 # 設置平台枚舉

    def _fetch_urls_by_params(self, params: Dict[str, Any], url_path: str) -> Generator[Dict[str, Any], None, None]:
        base_url = "https://www.yes123.com.tw/wk_index/"
        target_url = f"{base_url}{url_path}"

        page = 1
        while True: # 無限循環，直到沒有更多職缺或達到 max_pages 限制
            if self.cfg.max_pages is not None and page > self.cfg.max_pages:
                logger.info(f"[{self.platform.value}] 已達到最大頁數限制 ({self.cfg.max_pages})。")
                break

            if page > 1:
                params["strrec"] = (page - 1) * 20
            
            try:
                res = make_request(target_url, headers=self.cfg.headers, params=params, verify=False, delay=self.cfg.request_delay) # 傳遞延遲參數
                res.encoding = 'big5'
                soup = BeautifulSoup(res.text, "html.parser")
                
                selector = 'a[href^="job.asp?p_id="]'
                job_links = soup.select(selector)
                
                if not job_links:
                    logger.info(f"[{self.platform.value}] 在 URL {target_url} 參數 {params} 的第 {page} 頁未找到任何職缺連結。")
                    break
                
                for a_tag in job_links:
                    if href := a_tag.get('href'):
                        yield {"href": href}
                
                page += 1 # 頁數遞增

            except Exception as e:
                logger.error(f"[{self.platform.value}] 抓取 URL 列表頁面失敗 (URL: {target_url}, 參數: {params}, 頁數: {page}): {e}", exc_info=True)
                break

    def __call__(self) -> Generator[Dict[str, Any], None, None]:
        logger.info(f"[{self.platform.value}] 開始從資料庫獲取分類。")
        # 從資料庫獲取該平台的所有分類
        categories = repository.get_categories_by_platform(self.platform)

        if categories:
            logger.info(f"[{self.platform.value}] 開始為 {len(categories)} 個分類抓取 URL。")
            for cat in categories:
                if '_' not in cat.source_category_id:
                    continue
                logger.debug(f"[{self.platform.value}] 正在抓取分類: {cat.source_category_name} ({cat.source_category_id})")
                params = {
                    "find_work_mode1": cat.source_category_id,
                    "order_by": "m_date",
                    "order_ascend": "desc",
                }
                yield from self._fetch_urls_by_params(params, url_path="joblist.asp")
        else:
            logger.warning(f"[{self.platform.value}] 未提供任何分類，將回退到通用總覽頁抓取模式。")
            yield from self._fetch_urls_by_params({}, url_path="job.asp")

class HtmlDetailFetcher:
    def __init__(self, settings: Any):
        self.cfg = settings

    def __call__(self, url: str) -> str:
        try:
            res = make_request(url, headers=self.cfg.headers, verify=False, delay=self.cfg.request_delay) # 傳遞延遲參數
            return res.text
        except Exception as e:
            logger.error(f"[yes123] 獲取職缺詳情 HTML 失敗 for URL {url}: {e}")
            return ""

class HtmlDetailParser:
    def __call__(self, raw_content: str, url: str, intermediate_data: Optional[Dict[str, Any]]) -> Optional[Job]:
        return parsers.transform_details_to_job_model(raw_content, url)