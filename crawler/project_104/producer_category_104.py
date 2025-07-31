from .task_category_104 import fetch_url_data_104
import structlog

from crawler.logging_config import configure_logging
from crawler.project_104.config_104 import JOB_CAT_URL_104  # Changed import path

configure_logging()
logger = structlog.get_logger(__name__)

# 這段代碼保持原樣，用於在 Celery 環境中異步分派任務
fetch_url_data_104.s(JOB_CAT_URL_104).apply_async(queue='producer_category_104')
logger.info("send task_category_104 url", url=JOB_CAT_URL_104, queue='producer_category_104')


