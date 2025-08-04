from crawler.project_yes123.task_category_yes123 import fetch_and_sync_yes123_categories
from crawler.database.schemas import SourcePlatform
import structlog

from crawler.logging_config import configure_logging
from crawler.project_yes123.config_yes123 import JOB_CAT_URL_YES123

configure_logging()
logger = structlog.get_logger(__name__)

# 這段代碼保持原樣，用於在 Celery 環境中異步分派任務
fetch_and_sync_yes123_categories.s(JOB_CAT_URL_YES123).apply_async(queue='producer_category_yes123')
logger.info(
    "Sending task_category_yes123 URL.",
    event="send_category_task",
    url=JOB_CAT_URL_YES123,
    queue='producer_category_yes123',
    platform=SourcePlatform.PLATFORM_YES123,
    component="producer",
)