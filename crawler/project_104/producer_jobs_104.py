import structlog
import time

from crawler.project_104.task_jobs_104 import fetch_url_data_104
from crawler.database.repository import get_unprocessed_urls
from crawler.database.models import SourcePlatform
from crawler.database.connection import initialize_database

logger = structlog.get_logger(__name__)

# 設定每次從資料庫讀取的 URL 數量
BATCH_SIZE = 100

def dispatch_job_urls():
    initialize_database()
    logger.info("開始從資料庫讀取未處理的職缺 URL 並分發任務...")

    while True:
        # 從資料庫獲取一批未處理的 URL
        urls_to_process = get_unprocessed_urls(SourcePlatform.PLATFORM_104, BATCH_SIZE)

        if not urls_to_process:
            logger.info("所有未處理的職缺 URL 已分發完畢。")
            break

        for url_obj in urls_to_process:
            logger.info("分發職缺 URL 任務", url=url_obj.source_url)
            fetch_url_data_104.delay(url_obj.source_url)
        
        logger.info("已分發一批職缺 URL", count=len(urls_to_process))
        time.sleep(1) # 避免過於頻繁的資料庫查詢和任務分發

