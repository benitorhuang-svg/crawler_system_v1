import os
import sys
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import OperationalError
import structlog

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from crawler.config import MYSQL_DATABASE, MYSQL_HOST, MYSQL_PORT, MYSQL_ACCOUNT, MYSQL_PASSWORD

logger = structlog.get_logger(__name__)

def add_columns_to_job_observations_table():
    db_url = f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(db_url)

    column_definitions = {
        "region": "VARCHAR(255)",
        "district": "VARCHAR(255)",
        "latitude": "VARCHAR(20)",
        "longitude": "VARCHAR(20)",
        "skills": "TEXT", # Using TEXT for potentially longer skill strings
    }

    try:
        with engine.connect() as connection:
            inspector = inspect(engine)
            existing_columns = [col['name'] for col in inspector.get_columns('tb_job_observations')]

            for column_name, column_type in column_definitions.items():
                if column_name not in existing_columns:
                    alter_table_sql = text(f"ALTER TABLE tb_job_observations ADD COLUMN {column_name} {column_type}")
                    connection.execute(alter_table_sql)
                    logger.info(f"Added column '{column_name}' to 'tb_job_observations' table.")
                else:
                    logger.info(f"Column '{column_name}' already exists in 'tb_job_observations' table. Skipping.")
            connection.commit()
        logger.info("Database schema update completed successfully.")
    except OperationalError as e:
        logger.error(f"Database connection failed or operation error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Configure structlog for console output
    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    structlog.stdlib.reconfigure(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    )

    logger.info("Starting database schema migration for tb_job_observations.")
    add_columns_to_job_observations_table()
    logger.info("Finished database schema migration for tb_job_observations.")
