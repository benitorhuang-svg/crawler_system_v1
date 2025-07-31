import os
import sys
import pandas as pd
import structlog
from sqlalchemy import create_engine

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

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
    Demonstrates connecting to the database and reading data using Pandas.
    """
    db_url = (
        f"mysql+mysqlconnector://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )

    engine = create_engine(db_url)

    try:
        table_name = "tb_category_source"
        logger.info(
            "Attempting to read data from database using Pandas.", table=table_name
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
        engine.dispose()
        logger.info("Database engine disposed.")


if __name__ == "__main__":
    # To run this script for the test database, set the environment variable:
    # CRAWLER_DB_NAME=test_db python -m crawler.database.scripts.pandas_sql_config
    main()