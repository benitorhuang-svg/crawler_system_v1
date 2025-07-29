import pandas as pd
import requests

from ..worker import app
from crawler.database.connection import get_engine


def upload_data_to_mysql(df: pd.DataFrame):
    engine = get_engine()

    # 建立連線（可用於 Pandas、原生 SQL 操作）
    connect = engine.connect()

    df.to_sql(
        "TaiwanStockPrice",
        con=connect,
        if_exists="append",
        index=False,
    )


# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def crawler_finmind(stock_id):
    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": "2024-01-01",
        "end_date": "2025-06-17",
    }
    resp = requests.get(url, params=parameter)
    data = resp.json()
    if resp.status_code == 200:
        df = pd.DataFrame(data["data"])
        print(df)
        # print("upload db")
        upload_data_to_mysql(df)
    else:
        print(data["msg"])


if __name__ == "__main__":
    # This block is for testing purposes only, to simulate the task execution.
    # In a real Celery setup, this task would be invoked by a worker.
    crawler_finmind("2330")  # Using TSMC's stock ID as an example