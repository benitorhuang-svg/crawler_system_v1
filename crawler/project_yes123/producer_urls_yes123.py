from crawler.database.repository import get_all_categories_for_platform
from crawler.project_yes123.task_urls_yes123 import crawl_and_store_yes123_category_urls
from crawler.database.models import SourcePlatform
import structlog

from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

logger.info("Starting URL task distribution for all yes123 categories.")

all_yes123_categories = get_all_categories_for_platform(SourcePlatform.PLATFORM_YES123)

if all_yes123_categories:
    logger.info("Found categories for PLATFORM_YES123.", count=len(all_yes123_categories))
    root_categories = [
        cat for cat in all_yes123_categories if cat.parent_source_id is None
    ]

    if root_categories:
        logger.info(
            "Found root categories for PLATFORM_YES123.", count=len(root_categories)
        )

        for category_info in root_categories:
            category_id: str = category_info.source_category_id
            logger.info("分發 URL 抓取任務", category_id=category_id)
            crawl_and_store_yes123_category_urls.delay(category_info.model_dump())
    else:
        logger.info("No root categories found for PLATFORM_YES123.")
else:
    logger.info("No categories found for PLATFORM_YES123.")