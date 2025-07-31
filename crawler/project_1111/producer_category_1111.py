from crawler.project_1111.task_category_1111 import fetch_and_sync_1111_categories
import structlog

from crawler.logging_config import configure_logging
from crawler.project_1111.config_1111 import JOB_CAT_URL_1111

configure_logging()
logger = structlog.get_logger(__name__)

# 這段代碼保持原樣，用於在 Celery 環境中異步分派任務
fetch_and_sync_1111_categories.s(JOB_CAT_URL_1111).apply_async(queue='producer_category_1111')
logger.info("send task_category_1111 url", url=JOB_CAT_URL_1111, queue='producer_category_1111')