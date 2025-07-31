from crawler.database.repository import get_all_categories_for_platform
from crawler.project_1111.task_urls_1111 import crawl_and_store_1111_category_urls
from crawler.database.models import SourcePlatform
import structlog

from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

logger.info("Starting URL task distribution for all 1111 categories.")

all_1111_categories = get_all_categories_for_platform(SourcePlatform.PLATFORM_1111)

if all_1111_categories:
    logger.info("Found categories for PLATFORM_1111.", count=len(all_1111_categories))
    root_categories = [
        cat for cat in all_1111_categories if cat.parent_source_id is None
    ]

    if root_categories:
        logger.info(
            "Found root categories for PLATFORM_1111.", count=len(root_categories)
        )

        for category_info in root_categories:
            category_id: str = category_info.source_category_id
            logger.info("分發 URL 抓取任務", category_id=category_id)
            crawl_and_store_1111_category_urls.delay(category_info.model_dump())
    else:
        logger.info("No root categories found for PLATFORM_1111.")
else:
    logger.info("No categories found for PLATFORM_1111.")