from crawler.project_cakeresume.task_category_cakeresume import fetch_and_sync_cakeresume_categories
import structlog

from crawler.logging_config import configure_logging
from crawler.project_cakeresume.config_cakeresume import JOB_CAT_URL_CAKERESUME

configure_logging()
logger = structlog.get_logger(__name__)

# 這段代碼保持原樣，用於在 Celery 環境中異步分派任務
fetch_and_sync_cakeresume_categories.s(JOB_CAT_URL_CAKERESUME).apply_async(queue='producer_category_cakeresume')
logger.info("send task_category_cakeresume url", url=JOB_CAT_URL_CAKERESUME, queue='producer_category_cakeresume')