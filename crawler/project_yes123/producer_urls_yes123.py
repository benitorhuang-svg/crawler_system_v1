from crawler.database.repository import get_all_categories_for_platform
from crawler.project_yes123.task_urls_yes123 import crawl_and_store_yes123_category_urls
from crawler.database.models import SourcePlatform
import structlog

from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

logger.info(
    "Starting URL task distribution for all yes123 categories.",
    event="start_url_task_distribution",
    platform=SourcePlatform.PLATFORM_YES123,
    component="producer",
)

all_yes123_categories = get_all_categories_for_platform(SourcePlatform.PLATFORM_YES123)

if all_yes123_categories:
    logger.info(
        "Found categories for PLATFORM_YES123.",
        event="categories_found",
        count=len(all_yes123_categories),
        platform=SourcePlatform.PLATFORM_YES123,
        component="producer",
    )
    root_categories = [
        cat for cat in all_yes123_categories if cat.parent_source_id is None
    ]

    if root_categories:
        logger.info(
            "Found root categories for PLATFORM_YES123.", count=len(root_categories)
        )

        for category_info in root_categories:
            category_id: str = category_info.source_category_id
            logger.info(
            "Dispatching URL crawling task.",
            event="dispatch_url_crawling_task",
            category_id=category_id,
            platform=SourcePlatform.PLATFORM_YES123,
            component="producer",
        )
            crawl_and_store_yes123_category_urls.delay(category_info.model_dump())
    else:
        logger.info(
        "No root categories found for PLATFORM_YES123.",
        event="no_root_categories_found",
        platform=SourcePlatform.PLATFORM_YES123,
        component="producer",
    )
else:
    logger.info(
        "No categories found for PLATFORM_YES123.",
        event="no_categories_found",
        platform=SourcePlatform.PLATFORM_YES123,
        component="producer",
    )