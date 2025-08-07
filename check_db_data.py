import os
import sys
import structlog

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from crawler.database.connection import initialize_database, get_session
from crawler.database.models import Job
from crawler.database.schemas import SourcePlatform
from sqlalchemy import select

# 配置日誌
structlog.configure(
    processors=[
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger(__name__)

# 設定資料庫名稱環境變數
os.environ['CRAWLER_DB_NAME'] = 'db_YES123'

if __name__ == "__main__":
    logger.info("開始檢查資料庫中的職位資料...")
    try:
        initialize_database() # 確保資料庫和表格存在
        
        with get_session() as session:
            # 查詢 tb_jobs 表格中屬於 PLATFORM_YES123 的記錄數量
            count_statement = select(Job).where(Job.source_platform == SourcePlatform.PLATFORM_YES123)
            job_count = len(session.scalars(count_statement).all())
            logger.info(f"在 db_YES123 的 tb_jobs 表格中找到 {job_count} 條來自 YES123 平台的職位記錄。")

            if job_count > 0:
                # 查詢前 5 條記錄
                sample_statement = select(Job).where(Job.source_platform == SourcePlatform.PLATFORM_YES123).limit(5)
                sample_jobs = session.scalars(sample_statement).all()
                logger.info("前 5 條 YES123 職位記錄範例：")
                for job in sample_jobs:
                    logger.info(f"  ID: {job.source_job_id}, 標題: {job.title}")
            else:
                logger.info("沒有找到來自 YES123 平台的職位記錄。")

    except Exception as e:
        logger.error(f"檢查資料庫時發生錯誤: {e}", exc_info=True)
