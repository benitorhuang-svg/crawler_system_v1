import pandas as pd
import structlog
from sqlalchemy import create_engine

from crawler.logging_config import configure_logging
from crawler.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_ACCOUNT,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

configure_logging()
logger = structlog.get_logger(__name__)

def main():
    """
    示範如何透過 Pandas 直接連線到資料庫並讀取資料。
    """
    # 建立資料庫連接 URL
    # 使用 mysql+mysqlconnector 驅動
    db_url = (
        f"mysql+mysqlconnector://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )

    # 建立 SQLAlchemy 引擎
    engine = create_engine(db_url)

    try:
        # 嘗試從資料庫讀取一個範例資料表
        # 請替換 'tb_category_source' 為你實際想要查詢的資料表名稱
        # 如果資料庫中沒有 'tb_category_source'，請替換為其他存在的資料表
        table_name = "tb_category_source"
        logger.info("Attempting to read data from database using Pandas.", table=table_name)
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", engine)

        logger.info("Successfully read data from database.", table=table_name, rows_read=len(df))
        logger.info("DataFrame head:", dataframe_head=df.head().to_dict('records'))

    except Exception as e:
        logger.error("An error occurred during database connection or query.", error=e, exc_info=True)

    finally:
        # 關閉引擎連接池
        engine.dispose()
        logger.info("Database engine disposed.")

if __name__ == "__main__":
    # python -m crawler.database.pandas_sql_config
    main()
