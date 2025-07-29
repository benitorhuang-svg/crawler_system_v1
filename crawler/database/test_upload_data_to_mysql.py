import pandas as pd
import requests
import structlog
from datetime import datetime

from ..worker import app
from crawler.database.connection import get_session
from crawler.logging_config import configure_logging
from crawler.finmind.config import ( # Changed import path
    FINMIND_API_BASE_URL,
    FINMIND_START_DATE,
    FINMIND_END_DATE,
)

configure_logging()
logger = structlog.get_logger(__name__)

def upload_data_to_mysql(df: pd.DataFrame):
    if df.empty:
        logger.info("DataFrame is empty, skipping upload to MySQL.")
        return

    try:
        with get_session() as session:
            df.to_sql(
                "TaiwanStockPrice",
                con=session.connection(),
                if_exists="append",
                index=False,
            )
            logger.info("Data uploaded to MySQL successfully.", table="TaiwanStockPrice", rows_uploaded=len(df))
    except Exception as e:
        logger.error("Failed to upload data to MySQL.", error=e, exc_info=True)
        raise

@app.task()
def crawler_finmind(stock_id: str):
    logger.info("Starting FinMind data crawl.", stock_id=stock_id)

    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": FINMIND_START_DATE,
        "end_date": FINMIND_END_DATE,
    }

    try:
        resp = requests.get(FINMIND_API_BASE_URL, params=parameter, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if resp.status_code == 200:
            df = pd.DataFrame(data.get("data", []))
            logger.info("FinMind API data fetched.", stock_id=stock_id, rows_fetched=len(df))
            upload_data_to_mysql(df)
        else:
            logger.error("FinMind API returned an error.", status_code=resp.status_code, message=data.get("msg"), stock_id=stock_id)
    except requests.exceptions.RequestException as e:
        logger.error("Network or API request error.", error=e, stock_id=stock_id, exc_info=True)
    except Exception as e:
        logger.error("An unexpected error occurred during FinMind crawl.", error=e, stock_id=stock_id, exc_info=True)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        logger.info("Usage: python -m crawler.database.test_upload_data_to_mysql <stock_id>")
        sys.exit(1)

    stock_id_for_test = sys.argv[1]
    logger.info("Dispatching crawler_finmind task for local testing.", stock_id=stock_id_for_test)
    crawler_finmind.delay(stock_id_for_test)
