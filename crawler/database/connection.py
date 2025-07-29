import logging
import structlog
from contextlib import contextmanager

from tenacity import retry, stop_after_attempt, wait_exponential, before_log, RetryError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 直接從集中的設定模組導入，不再重複讀取 .ini
from crawler.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_ACCOUNT,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)
from crawler.database.models import Base

# --- 核心設定 ---
logger = structlog.get_logger(__name__)
metadata = Base.metadata
_engine = None  # 使用單例模式確保 Engine 只被建立一次

# SessionLocal 將在 get_session 中與 engine 綁定
SessionLocal = sessionmaker(autocommit=False, autoflush=False)


# --- 核心功能 ---


@contextmanager
def get_session():
    """
    提供一個資料庫 Session 的上下文管理器。
    它能自動處理 commit、rollback 和 session 關閉。
    """
    engine = get_engine()  # 確保 engine 已被初始化
    SessionLocal.configure(bind=engine)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        logger.error("Session 發生錯誤，執行回滾 (rollback)", exc_info=True)
        session.rollback()
        raise
    finally:
        session.close()


def get_engine():
    """
    獲取 SQLAlchemy 引擎實例，帶有連接重試機制。
    這是一個單例，確保在應用程式生命週期中只創建一次引擎。
    """
    global _engine
    if _engine is None:
        try:
            _engine = _connect_with_retry()
        except RetryError as e:
            logger.critical(
                "資料庫連接在多次重試後失敗，應用程式無法啟動。", error=e, exc_info=True
            )
            raise RuntimeError("資料庫連接失敗，請檢查資料庫服務是否正常。") from e
        except Exception as e:
            logger.critical(
                "創建資料庫引擎時發生未預期的錯誤。", error=e, exc_info=True
            )
            raise RuntimeError("創建資料庫引擎時發生致命錯誤。") from e
    return _engine


@retry(
    stop=stop_after_attempt(8),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    before=before_log(logger, logging.INFO),
    reraise=True,
)
def _connect_with_retry():
    """
    （內部函式）執行實際的資料庫連接，由 tenacity 提供重試能力。
    """
    logger.info(f"正在嘗試連接到資料庫: {MYSQL_DATABASE}@{MYSQL_HOST}:{MYSQL_PORT}")

    # 假設資料庫已存在，直接連接
    # 使用 pymysql 驅動，並設定 utf8mb4 字符集
    db_url = (
        f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset=utf8mb4"
    )

    engine = create_engine(
        db_url,
        pool_recycle=3600,  # 每小時回收一次連接，防止連接被 MySQL 伺服器中斷
        echo=False,
        connect_args={"connect_timeout": 10},
        isolation_level="READ COMMITTED",
    )

    # 測試連接，如果失敗會觸發 tenacity 重試
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    logger.info("資料庫引擎創建成功，連接測試通過。")
    return engine


def initialize_database():
    """
    初始化資料庫，根據 models.py 中的定義創建所有資料表。
    此函式應在應用程式啟動時或透過專門的腳本手動調用。
    """
    logger.info("正在初始化資料庫，檢查並創建所有資料表...")
    try:
        engine = get_engine()
        metadata.create_all(engine)
        logger.info("資料庫資料表初始化完成。")
    except Exception as e:
        logger.critical("初始化資料庫資料表失敗。", error=e, exc_info=True)
        raise
