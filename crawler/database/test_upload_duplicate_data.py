# 匯入 SQLAlchemy 所需模組
# 匯入 pandas 並建立一個 DataFrame，模擬要寫入的資料
import pandas as pd  # 用來處理資料表（DataFrame）
import structlog
from sqlalchemy import (
    Column,
    Date,
    Float,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.mysql import (
    insert,
)  # 專用於 MySQL 的 insert 語法，可支援 on_duplicate_key_update

from crawler.logging_config import configure_logging
from crawler.database.connection import get_engine

configure_logging()

engine = get_engine()

# 定義資料表結構，對應到 MySQL 中的 test_duplicate 表
metadata = MetaData()
stock_price_table = Table(
    "test_duplicate",  # 資料表名稱
    metadata,
    Column("stock_id", String(50), primary_key=True),  # 主鍵 stock_id 欄位
    Column("date", Date, primary_key=True),
    Column("price", Float),
)
# ✅ 自動建立資料表（如果不存在才建立）
metadata.create_all(engine)

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

# 遍歷 DataFrame 的每一列資料
for _, row in df.iterrows():
    # 使用 SQLAlchemy 的 insert 語句建立插入語法
    insert_stmt = insert(stock_price_table).values(**row.to_dict())

    # 加上 on_duplicate_key_update 的邏輯：
    # 若主鍵重複（id 已存在），就更新 name 與 score 欄位為新值
    update_stmt = insert_stmt.on_duplicate_key_update(
        **{
            col.name: insert_stmt.inserted[col.name]
            for col in stock_price_table.columns
            if col.name != "id"
        }
    )

    # 執行 SQL 語句，寫入資料庫
    with engine.begin() as connection:
        connection.execute(update_stmt)

# 從資料庫讀取資料並列印
read_df = pd.read_sql("SELECT * FROM test_duplicate", con=engine)
structlog.get_logger(__name__).info(f"Data read from database:\n{read_df}")