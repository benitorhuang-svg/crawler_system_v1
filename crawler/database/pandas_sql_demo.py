import pandas as pd
import structlog
from sqlalchemy import create_engine

from crawler.logging_config import configure_logging
# from crawler.config import ( # 暫時不從 config 匯入，直接硬編碼用於測試
#     MYSQL_HOST,
#     MYSQL_PORT,
#     MYSQL_ACCOUNT,
#     MYSQL_PASSWORD,
#     MYSQL_DATABASE,
# )

configure_logging()
logger = structlog.get_logger(__name__)


def main():
    """
    示範如何透過 Pandas 直接連線到資料庫並讀取資料。
    """
    # --- 僅用於本次測試的硬編碼連線資訊 ---
    # 在實際應用中，這些值應從 crawler.config 匯入
    test_mysql_host = "127.0.0.1"
    test_mysql_port = 3306
    test_mysql_account = "root"
    test_mysql_password = "root_password"
    test_mysql_database = "crawler_db"
    # ---------------------------------------

    # 建立資料庫連接 URL
    # 使用 mysql+mysqlconnector 驅動
    db_url = (
        f"mysql+mysqlconnector://{test_mysql_account}:{test_mysql_password}@"
        f"{test_mysql_host}:{test_mysql_port}/{test_mysql_database}"
    )

    # 建立 SQLAlchemy 引擎
    engine = create_engine(db_url)

    try:
        # 嘗試從資料庫讀取一個範例資料表
        # 請替換 'tb_category_source' 為你實際想要查詢的資料表名稱
        # 如果資料庫中沒有 'tb_category_source'，請替換為其他存在的資料表
        table_name = "tb_category_source"
        logger.info(
            "Attempting to read data from database using Pandas (hardcoded for test).",
            table=table_name,
        )
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", engine)

        logger.info(
            "Successfully read data from database.", table=table_name, rows_read=len(df)
        )
        logger.info("DataFrame head:", dataframe_head=df.head().to_dict("records"))

    except Exception as e:
        logger.error(
            "An error occurred during database connection or query.",
            error=e,
            exc_info=True,
        )

    finally:
        # 關閉引擎連接池
        engine.dispose()
        logger.info("Database engine disposed.")


if __name__ == "__main__":
    # python -m crawler.database.pandas_sql_demo
    main()
