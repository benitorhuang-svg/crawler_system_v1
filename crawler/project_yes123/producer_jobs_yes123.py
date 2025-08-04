import structlog
from celery import group
from sqlalchemy.exc import SQLAlchemyError

from crawler.project_yes123.task_jobs_yes123 import fetch_url_data_yes123
from crawler.database.repository import get_urls_by_crawl_status, mark_urls_as_queued
from crawler.database.schemas import CrawlStatus, SourcePlatform
from crawler.config import PRODUCER_BATCH_SIZE

from crawler.logging_config import configure_logging

# --- 初始化 ---
configure_logging()
logger = structlog.get_logger(__name__)

logger.info(
    "Producer configuration loaded.",
    event="producer_config_loaded",
    producer_batch_size=PRODUCER_BATCH_SIZE,
    platform=SourcePlatform.PLATFORM_YES123,
    component="producer",
)


def dispatch_yes123_job_urls():
    """
    從資料庫讀取待處理或失敗的 yes123 職缺 URL，更新其狀態，然後分發給 Celery worker。
    """
    logger.info(
        "Starting to read yes123 job URLs from database and dispatch tasks...",
        event="start_dispatching_job_tasks",
        platform=SourcePlatform.PLATFORM_YES123,
        component="producer",
    )

    try:
        # 1. 讀取新任務 (PENDING) 和失敗的任務 (FAILED)
        statuses_to_fetch = [CrawlStatus.FAILED, CrawlStatus.PENDING]
        urls_to_process = get_urls_by_crawl_status(
            platform=SourcePlatform.PLATFORM_YES123,
            statuses=statuses_to_fetch,
            limit=PRODUCER_BATCH_SIZE,
        )

        if not urls_to_process:
            logger.info(
                "No eligible yes123 job URLs found to dispatch.",
                event="no_eligible_urls_found",
                platform=SourcePlatform.PLATFORM_YES123,
                component="producer",
            )
            return

        logger.info(
            "Fetched a batch of yes123 URLs from database.",
            event="fetched_url_batch",
            count=len(urls_to_process),
            platform=SourcePlatform.PLATFORM_YES123,
            component="producer",
        )

        # 2. 立即更新這些 URL 的狀態為 QUEUED，防止其他 producer 重複讀取
        mark_urls_as_queued(SourcePlatform.PLATFORM_YES123, urls_to_process)
        logger.info(
            "Updated yes123 URL status to QUEUED.",
            event="urls_status_queued",
            count=len(urls_to_process),
            platform=SourcePlatform.PLATFORM_YES123,
            component="producer",
        )

        # 3. 使用 group 高效地批次分發任務，並指定佇列
        task_group = group(fetch_url_data_yes123.s(url.source_url) for url in urls_to_process)
        task_group.apply_async(queue="producer_jobs_yes123")

        logger.info(
            "Successfully dispatched a batch of yes123 job URL tasks.",
            event="job_tasks_dispatched",
            count=len(urls_to_process),
            queue="producer_jobs_yes123",
            platform=SourcePlatform.PLATFORM_YES123,
            component="producer",
        )

    except SQLAlchemyError as e:
        logger.error("Database operation failed.",
            event="database_operation_failed",
            error=str(e),
            platform=SourcePlatform.PLATFORM_YES123,
            component="producer",
            exc_info=True,
        )
    except Exception as e:
        logger.error("An unexpected error occurred while dispatching tasks.",
            event="unexpected_dispatch_error",
            error=str(e),
            platform=SourcePlatform.PLATFORM_YES123,
            component="producer",
            exc_info=True,
        )

if __name__ == "__main__":
    dispatch_yes123_job_urls()