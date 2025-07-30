from crawler.database.repository import get_all_categories_for_platform
from crawler.project_104.task_urls_104 import crawl_and_store_category_urls
from crawler.database.models import SourcePlatform
import structlog

from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

def dispatch_urls_for_all_categories(limit: int = 0, sort_key: Optional[str] = None) -> None:
    """
    分發所有 104 職務類別的 URL 抓取任務。

    從資料庫中獲取所有 104 平台的類別，並為每個類別分發一個 Celery 任務，
    由 `crawl_and_store_category_urls` 任務負責實際的 URL 抓取。

    :param limit: 限制分發的類別數量。0 表示無限制。
    :param sort_key: 用於排序類別的鍵 (例如 'source_category_id', 'source_category_name')。
    :return: 無。
    :rtype: None
    """
    logger.info("Starting URL task distribution for all 104 categories.", limit=limit, sort_key=sort_key)

    all_104_categories = get_all_categories_for_platform(SourcePlatform.PLATFORM_104)

    if all_104_categories:
        logger.info("Found categories for PLATFORM_104.", count=len(all_104_categories))
        root_categories = [
            cat for cat in all_104_categories if cat.parent_source_id is None
        ]

        if root_categories:
            logger.info(
                "Found root categories for PLATFORM_104.", count=len(root_categories)
            )

            # 根據 sort_key 排序
            if sort_key and hasattr(root_categories[0], sort_key):
                root_categories.sort(key=lambda x: getattr(x, sort_key))
                logger.info("Categories sorted.", sort_key=sort_key)

            categories_to_dispatch = root_categories
            if limit > 0:
                categories_to_dispatch = root_categories[:limit]
                logger.info(
                    "Applying category limit for dispatch.",
                    limit=limit,
                    actual_count=len(categories_to_dispatch),
                )

            for category_info in categories_to_dispatch:
                category_id: str = category_info.source_category_id
                logger.info("分發 URL 抓取任務", category_id=category_id)
                crawl_and_store_category_urls.s(
                    category_id, queue="producer_urls_104"
                )  # 指定佇列
        else:
            logger.info("No root categories found for PLATFORM_104.")
    else:
        logger.info("No categories found for PLATFORM_104.")


if __name__ == "__main__":
    dispatch_urls_for_all_categories()
