import os
import logging
import structlog
from contextlib import contextmanager

from tenacity import retry, stop_after_attempt, wait_exponential, before_log, RetryError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from crawler.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_ACCOUNT,
    MYSQL_PASSWORD,
    MYSQL_DATABASE as DEFAULT_DB_NAME,
)
from crawler.database.models import Base

logger = structlog.get_logger(__name__)
metadata = Base.metadata
_engine = None  # Singleton engine instance

SessionLocal = sessionmaker(autocommit=False, autoflush=False)


def get_db_name() -> str:
    """
    Determines the database name to use.
    It prioritizes the CRAWLER_DB_NAME environment variable for testing purposes.
    Otherwise, it falls back to the default database name from the config.
    """
    return os.environ.get('CRAWLER_DB_NAME', DEFAULT_DB_NAME)


@contextmanager
def get_session():
    """
    Provides a transactional database session via a context manager.
    Handles commit, rollback, and closing automatically.
    """
    engine = get_engine()  # Ensure engine is initialized
    SessionLocal.configure(bind=engine)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        logger.error("Session encountered an error, performing rollback.", exc_info=True)
        session.rollback()
        raise
    finally:
        session.close()


def get_engine():
    """
    Retrieves the SQLAlchemy engine instance, creating it if it doesn't exist.
    This is a singleton to ensure one engine instance per application lifecycle.
    """
    global _engine
    if _engine is None:
        try:
            _engine = _connect_with_retry()
        except RetryError as e:
            logger.critical(
                "Database connection failed after multiple retries. Application cannot start.",
                error=e,
                exc_info=True,
            )
            raise RuntimeError("Database connection failed. Please check the database service.") from e
        except Exception as e:
            logger.critical(
                "An unexpected error occurred while creating the database engine.",
                error=e,
                exc_info=True,
            )
            raise RuntimeError("Fatal error creating the database engine.") from e
    return _engine


@retry(
    stop=stop_after_attempt(8),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    before=before_log(logger, logging.INFO),
    reraise=True,
)
def _connect_with_retry() -> create_engine:
    """
    (Internal) Performs the actual database connection with retry logic.
    """
    db_name = get_db_name()
    logger.info(f"Attempting to connect to database: {db_name}@{MYSQL_HOST}:{MYSQL_PORT}")

    db_url = (
        f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{db_name}?charset=utf8mb4"
    )

    engine = create_engine(
        db_url,
        pool_recycle=3600,
        echo=False,
        connect_args={"connect_timeout": 10},
        isolation_level="READ COMMITTED",
    )

    # Test the connection; this will trigger tenacity's retry if it fails
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    logger.info("Database engine created successfully, connection test passed.")
    return engine


def initialize_database():
    """
    Initializes the database. If the target is 'test_db', it ensures
    the database exists before creating tables. For other databases,
    it simply creates tables based on the models.
    """
    db_name = get_db_name()
    logger.info(f"Initializing database: {db_name}")

    # If using the test database, ensure it exists first.
    if db_name == 'test_db':
        server_db_url = (
            f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@"
            f"{MYSQL_HOST}:{MYSQL_PORT}/?charset=utf8mb4"
        )
        server_engine = create_engine(server_db_url)
        try:
            with server_engine.connect() as connection:
                connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name};"))
                connection.commit()
            logger.info(f"Ensured database '{db_name}' exists.")
        finally:
            server_engine.dispose()

    # Now, connect to the specific database and create all tables
    try:
        engine = get_engine()
        metadata.create_all(engine)
        logger.info(f"Database tables for '{db_name}' initialized successfully.")
    except Exception as e:
        logger.critical(
            f"Failed to initialize tables for database '{db_name}'.",
            error=e,
            exc_info=True,
        )
        raise