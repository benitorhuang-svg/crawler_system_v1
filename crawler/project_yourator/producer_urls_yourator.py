from crawler.database.repository import get_all_categories_for_platform
from crawler.project_yourator.task_urls_yourator import crawl_and_store_yourator_category_urls
from crawler.database.schemas import SourcePlatform
import structlog

from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

logger.info("Starting URL task distribution for all Yourator categories.")

all_yourator_categories = get_all_categories_for_platform(SourcePlatform.PLATFORM_YOURATOR)

if all_yourator_categories:
    logger.info("Found categories for PLATFORM_YOURATOR.", count=len(all_yourator_categories))
    for category_info in all_yourator_categories:
        category_id: str = category_info.source_category_id
        logger.info("分發 URL 抓取任務", category_id=category_id)
        crawl_and_store_yourator_category_urls.delay(category_info.model_dump())
else:
    logger.info("No categories found for PLATFORM_YOURATOR.")
