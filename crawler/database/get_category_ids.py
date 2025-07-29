import structlog
import pandas as pd
import os

from crawler.database.connection import (
    get_session,
    initialize_database,
)  # Import get_session
from crawler.database.models import CategorySource  # Import the model
from crawler.logging_config import configure_logging  # Import configure_logging

configure_logging()  # Configure logging at the script level
logger = structlog.get_logger(__name__)


def get_source_category_ids():
    """
    從資料庫獲取所有職務分類的 ID 和名稱，並以 Pandas DataFrame 形式返回。
    """
    try:
        with get_session() as session:  # Use get_session context manager
            # 使用 SQLAlchemy ORM 查詢資料
            categories = session.query(CategorySource).all()

            # 將 ORM 物件轉換為字典列表，然後再轉換為 DataFrame
            data = [
                {
                    "parent_source_id": cat.parent_source_id,
                    "source_category_id": cat.source_category_id,
                    "source_category_name": cat.source_category_name,
                }
                for cat in categories
            ]
            df = pd.DataFrame(data)
            logger.info("Successfully fetched source category IDs.", count=len(df))
            return df
    except Exception as e:
        logger.error(
            "Error fetching source_category_ids with ORM.", error=e, exc_info=True
        )
        return pd.DataFrame()  # 在錯誤時返回空的 DataFrame


if __name__ == "__main__":
    # Set APP_ENV for local testing
    os.environ["APP_ENV"] = "DEV"

    # 確保資料庫在本地測試時被初始化
    initialize_database()

    ids_df = get_source_category_ids()
    logger.info(
        "Source Category IDs fetched.", dataframe_head=ids_df.head().to_dict("records")
    )
    # 如果需要完整輸出，可以使用 print(ids_df.to_string())
