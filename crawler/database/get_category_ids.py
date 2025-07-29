from crawler.database.connection import get_engine, initialize_database
import structlog
import pandas as pd
import os

logger = structlog.get_logger(__name__)

def get_source_category_ids():
    initialize_database() # Ensure tables are created
    engine = get_engine()
    try:
        query = "SELECT parent_source_id, source_category_id, source_category_name FROM tb_category_source"
        df = pd.read_sql_query(query, engine)
        
        return df
    except Exception as e:
        logger.error("Error fetching source_category_ids with pandas", error=e, exc_info=True)
        return []

if __name__ == "__main__":
    # Set APP_ENV for local testing
    os.environ["APP_ENV"] = "DEV"
    
    ids = get_source_category_ids()
    print(f"Source Category IDs: {ids}")
