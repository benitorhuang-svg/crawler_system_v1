from .task_category_104 import fetch_url_data_104
import structlog

from crawler.logging_config import configure_logging
from crawler.project_104.config_104 import JOB_CAT_URL_104  # Changed import path

configure_logging()
logger = structlog.get_logger(__name__)

# 這段代碼保持原樣，用於在 Celery 環境中異步分派任務
# fetch_url_data_104.s(JOB_CAT_URL_104).apply_async(queue='producer_category_104')
# logger.info("send task_category_104 url", url=JOB_CAT_URL_104, queue='producer_category_104')

# 新增的部分：允許直接執行此腳本進行本地測試
if __name__ == "__main__":
    logger.info("Running producer_category_104.py directly for local testing.")
    logger.info("Fetching job categories from URL.", url=JOB_CAT_URL_104)
    
    # 直接調用函數，而不是作為 Celery 任務
    # 這樣可以在沒有 Celery worker 的情況下，同步執行抓取任務
    result = fetch_url_data_104(JOB_CAT_URL_104)
    
    if result:
        logger.info("Successfully fetched and processed job categories.", count=len(result))
    else:
        logger.error("Failed to fetch or process job categories.")
