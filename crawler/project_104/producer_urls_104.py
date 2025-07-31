from crawler.database.repository import get_all_categories_for_platform, get_all_crawled_category_ids_pandas, get_stale_crawled_category_ids_pandas
from crawler.project_104.task_urls_104 import crawl_and_store_category_urls
from crawler.database.models import SourcePlatform, CategorySourcePydantic
import structlog
from typing import Optional, Set, List


from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

def dispatch_urls_for_all_categories(sort_key: Optional[str] = None, limit: int = 0, url_limit: int = 0, n_days: int = 7) -> None:
    """
    分發所有 104 職務類別的 URL 抓取任務。

    從資料庫中獲取所有 104 平台的類別，並為每個類別分發一個 Celery 任務，
    由 `crawl_and_store_category_urls` 任務負責實際的 URL 抓取。

    :param sort_key: 用於排序類別的鍵 (例如 'source_category_id', 'source_category_name')。
    :param limit: 限制分發的類別數量。0 表示無限制。
    :param url_limit: 限制每個分類任務抓取的 URL 數量。0 表示無限制。
    :param n_days: 判斷類別是否需要重新爬取的時間間隔（天）。
    :return: 無。
    :rtype: None
    """
    logger.info("Starting URL task distribution for all 104 categories.",  sort_key=sort_key, limit=limit, url_limit=url_limit, n_days=n_days)

    # 1. 取出所有類別 A
    all_categories_pydantic: List[CategorySourcePydantic] = get_all_categories_for_platform(SourcePlatform.PLATFORM_104)
    all_category_ids: Set[str] = {cat.source_category_id for cat in all_categories_pydantic}

    # 2. 取出 tb_url_categories.source_category_id B
    all_crawled_category_ids: Set[str] = get_all_crawled_category_ids_pandas(SourcePlatform.PLATFORM_104)

    # 3. B 超過7天 視為 C
    stale_crawled_category_ids: Set[str] = get_stale_crawled_category_ids_pandas(SourcePlatform.PLATFORM_104, n_days)

    # 4. D = (A - B) | C
    categories_to_dispatch_ids = (all_category_ids - all_crawled_category_ids) | stale_crawled_category_ids

    # 5. 分發任務
    # Filter the full pydantic objects for dispatch
    categories_to_dispatch = [
        cat for cat in all_categories_pydantic 
        if cat.source_category_id in categories_to_dispatch_ids
    ]

    if categories_to_dispatch:
        logger.info("Found categories to dispatch.", count=len(categories_to_dispatch))
        
        if limit > 0:
            categories_to_dispatch = categories_to_dispatch[:limit]
            logger.info(
                "Applying category limit for dispatch.",
                limit=limit,
                actual_count=len(categories_to_dispatch),
            )

        for category_info in categories_to_dispatch:
            category_id: str = category_info.source_category_id
            logger.info("分發 URL 抓取任務", category_id=category_id, url_limit=url_limit)
            # 在直接執行模式下，我們直接調用函數
            crawl_and_store_category_urls(category_info.model_dump(), url_limit=url_limit)
    else:
        logger.info("No categories found to dispatch for URL crawling.")
