import structlog
from celery import group
from sqlalchemy.exc import SQLAlchemyError

from crawler.project_yes123.task_jobs_yes123 import fetch_url_data_yes123
from crawler.database.repository import get_urls_by_crawl_status, update_urls_status
from crawler.database.models import SourcePlatform, CrawlStatus
from crawler.logging_config import configure_logging
from crawler.config import PRODUCER_BATCH_SIZE

# --- 初始化 ---
configure_logging()
logger = structlog.get_logger(__name__)

logger.info("Producer configuration loaded.", producer_batch_size=PRODUCER_BATCH_SIZE)


def dispatch_yes123_job_urls():
    """
    從資料庫讀取待處理或失敗的 yes123 職缺 URL，更新其狀態，然後分發給 Celery worker。
    """
    logger.info("開始從資料庫讀取 yes123 職缺 URL 並分發任務...")

    try:
        # 1. 讀取新任務 (PENDING) 和失敗的任務 (FAILED)
        statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING]
        urls_to_process = get_urls_by_crawl_status(
            platform=SourcePlatform.PLATFORM_YES123,
            statuses=statuses_to_fetch,
            limit=PRODUCER_BATCH_SIZE,
        )

        if not urls_to_process:
            logger.info("沒有找到符合條件的 yes123 職缺 URL 可供分發。")
            return

        logger.info("從資料庫讀取到一批 yes123 URL", count=len(urls_to_process))

        # 2. 立即更新這些 URL 的狀態為 QUEUED，防止其他 producer 重複讀取
        update_urls_status(urls_to_process, CrawlStatus.QUEUED)
        logger.info("已更新 yes123 URL 狀態為 QUEUED", count=len(urls_to_process))

        # 3. 使用 group 高效地批次分發任務，並指定佇列
        task_group = group(fetch_url_data_yes123.s(url.source_url) for url in urls_to_process)
        task_group.apply_async(queue="producer_jobs_yes123")

        logger.info(
            "已成功分發一批 yes123 職缺 URL 任務", count=len(urls_to_process), queue="producer_jobs_yes123"
        )

    except SQLAlchemyError as e:
        logger.error("資料庫操作失敗", error=str(e))
    except Exception as e:
        logger.error("分發任務時發生未預期的錯誤", error=str(e))