import pandas as pd
import structlog
from sqlalchemy import (
    Column,
    Date,
    Float,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.mysql import insert

from crawler.logging_config import configure_logging
from crawler.database.connection import get_session, initialize_database # Import get_session and initialize_database

configure_logging()
logger = structlog.get_logger(__name__)

# 定義資料表結構，對應到 MySQL 中的 test_duplicate 表
metadata = MetaData()
stock_price_table = Table(
    "test_duplicate",  # 資料表名稱
    metadata,
    Column("stock_id", String(50), primary_key=True),  # 主鍵 stock_id 欄位
    Column("date", Date, primary_key=True),
    Column("price", Float),
)

# 建立 DataFrame，模擬要寫入的資料
df = pd.DataFrame(
    [
        # 模擬 5 筆重複資料
        {"stock_id": "2330", "date": "2025-06-25", "price": 1000},
        {"stock_id": "2330", "date": "2025-06-25", "price": 1001},
        {"stock_id": "2330", "date": "2025-06-25", "price": 1002},
        {"stock_id": "2330", "date": "2025-06-25", "price": 1003},
        {"stock_id": "2330", "date": "2025-06-25", "price": 1004},
    ]
)

if __name__ == "__main__":
    # 確保資料庫表在測試前被建立
    initialize_database()

    logger.info("Starting test for duplicate data upload with UPSERT.")

    try:
        with get_session() as session:
            # 使用 bulk insert with on_duplicate_key_update
            # 將 DataFrame 轉換為字典列表，以便 insert 語句處理
            insert_stmt = insert(stock_price_table).values(df.to_dict(orient="records"))

            # 定義在主鍵重複時要更新的欄位。這裡只更新 'price' 欄位。
            on_duplicate_update_dict = {
                "price": insert_stmt.inserted.price
            }

            final_stmt = insert_stmt.on_duplicate_key_update(**on_duplicate_update_dict)
            session.execute(final_stmt)
            # session.commit() 由 get_session 上下文管理器自動處理

        logger.info("Data upserted successfully.", rows_processed=len(df))

        # 從資料庫讀取資料並列印
        with get_session() as session:
            # pd.read_sql 可以直接使用 session 的 connection
            read_df = pd.read_sql("SELECT * FROM test_duplicate", con=session.connection())
            logger.info("Data read from database.", dataframe_content=read_df.to_dict(orient='records'))

    except Exception as e:
        logger.error("An error occurred during duplicate data test.", error=e, exc_info=True)
