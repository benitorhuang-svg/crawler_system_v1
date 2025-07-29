import structlog
import os
import sys
import logging
import configparser

from tenacity import retry, stop_after_attempt, wait_exponential, before_log, RetryError

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

# Add the parent directory to the Python path to allow importing 'crawler'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from crawler.database.models import Base

# Configure logging
logger = structlog.get_logger(__name__)

# Placeholder for settings.db based on environment variables
class DBSettings:
    def __init__(self):
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'local.ini')
        
        try:
            config.read(config_path)
        except Exception as e:
            logger.critical(f"無法讀取 local.ini 設定檔: {e}", exc_info=True)
            raise RuntimeError("無法讀取資料庫設定檔。") from e

        # Determine which section to use based on APP_ENV environment variable
        # Default to 'DEV' if APP_ENV is not set or invalid
        app_env = os.environ.get("APP_ENV", "DOCKER").upper()
        if app_env not in config:
            logger.warning(f"環境變數 APP_ENV={app_env} 無效或未找到對應區塊，預設使用 [DOCKER] 設定。")
            app_env = "DOCKER"

        db_section = config[app_env]

        self.user = db_section.get("MYSQL_ACCOUNT")
        self.password = db_section.get("MYSQL_PASSWORD")
        self.host = db_section.get("MYSQL_HOST")
        self.port = int(db_section.get("MYSQL_PORT"))
        self.database = db_section.get("MYSQL_DATABASE", "crawler_db") # Provide a default if not found in section.

settings = type('Settings', (object,), {'db': DBSettings()})()

_engine = None
metadata = Base.metadata # Use metadata from the Base in models.py

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=None) # bind will be set in get_session

@contextmanager
def get_session():
    engine = get_engine() # Ensure engine is initialized
    SessionLocal.configure(bind=engine) # Configure session with the engine
    session = SessionLocal()
    try:
        yield session
        session.commit() # 自動提交
    except Exception:
        session.rollback() # 發生錯誤時回滾
        raise
    finally:
        session.close()

def get_engine() -> create_engine:
    """
    獲取 SQLAlchemy 引擎實例，帶有強大的連接重試機制和正確的字元集配置。
    """
    global _engine
    if _engine is None:
        try:
            @retry(
                stop=stop_after_attempt(10),
                wait=wait_exponential(multiplier=1, min=2, max=30),
                before=before_log(logger, logging.INFO),
                reraise=True
            )
            def _connect_with_retry() -> create_engine:
                logger.info("正在嘗試創建 MySQL 引擎")
                db = settings.db
                
                # Connect without specifying a database to create it if it doesn't exist
                temp_addr = f"mysql+pymysql://{db.user}:{db.password}@{db.host}:{db.port}/?charset=utf8mb4"
                temp_engine = create_engine(temp_addr)
                
                try:
                    logger.debug(f"嘗試連接到 MySQL 伺服器以建立資料庫 '{db.database}'...")
                    with temp_engine.connect() as conn:
                        logger.debug(f"已連接到 MySQL 伺服器。執行 CREATE DATABASE IF NOT EXISTS {db.database}...")
                        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db.database} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
                        conn.commit()
                    logger.info(f"資料庫 '{db.database}' 已確認存在或已創建。")
                except Exception as e:
                    logger.error(f"無法創建或確認資料庫 '{db.database}': {e}", exc_info=True)
                    raise
                finally:
                    temp_engine.dispose() # Close the temporary connection pool
                
                # Now connect to the specific database
                addr = f"mysql+pymysql://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}?charset=utf8mb4"
                
                engine = create_engine(
                    addr,
                    pool_recycle=3600,
                    echo=False,
                    connect_args={'connect_timeout': 10}, # pymysql-specific
                    isolation_level="READ COMMITTED"
                )
                
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    
                logger.info("MySQL 引擎創建成功且連接測試通過")
                return engine

            _engine = _connect_with_retry()

        except RetryError as e:
            logger.critical("資料庫連接在多次重試後失敗。資料庫可能已關閉或無法訪問", error=e, exc_info=True)
            raise RuntimeError("資料庫連接在多次重試後失敗。") from e
        except Exception as e:
            logger.critical("創建資料庫引擎時發生意外錯誤", error=e, exc_info=True)
            raise RuntimeError("創建資料庫引擎時發生意外錯誤。") from e
            
    return _engine

def initialize_database() -> None:
    """
    初始化資料庫，創建所有定義的表結構。
    """
    logger.info("正在初始化資料庫表")
    try:
        engine = get_engine()
        # 確保數據庫本身的默認字符集也是 utf8mb4
        with engine.connect() as connection:
            connection.execute(text(f"ALTER DATABASE {settings.db.database} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
            connection.commit() # Need to commit DDL operations
            logger.info("資料庫的字符集已確認/修改為 utf8mb4", database=settings.db.database)
        
        metadata.create_all(engine)
        logger.info("資料庫表初始化檢查完成")
        
    except Exception as e:
        logger.critical("初始化資料庫表失敗", error=e, exc_info=True)
        raise