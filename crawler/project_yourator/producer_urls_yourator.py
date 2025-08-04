from crawler.database.repository import get_all_categories_for_platform
from crawler.project_yourator.task_urls_yourator import crawl_and_store_yourator_category_urls
from crawler.database.schemas import SourcePlatform
import structlog

from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

logger.info(
    "Starting URL task distribution for all Yourator categories.",
    event="start_url_task_distribution",
    platform=SourcePlatform.PLATFORM_YOURATOR,
    component="producer",
)

all_yourator_categories = get_all_categories_for_platform(SourcePlatform.PLATFORM_YOURATOR)

if all_yourator_categories:
    logger.info(
        "Found categories for PLATFORM_YOURATOR.",
        event="categories_found",
        count=len(all_yourator_categories),
        platform=SourcePlatform.PLATFORM_YOURATOR,
        component="producer",
    )
    for category_info in all_yourator_categories:
        category_id: str = category_info.source_category_id
        logger.info(
            "Dispatching URL crawling task.",
            event="dispatch_url_crawling_task",
            category_id=category_id,
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="producer",
        )
        crawl_and_store_yourator_category_urls.delay(category_info.model_dump())
else:
    logger.info(
        "No categories found for PLATFORM_YOURATOR.",
        event="no_categories_found",
        platform=SourcePlatform.PLATFORM_YOURATOR,
        component="producer",
    )
