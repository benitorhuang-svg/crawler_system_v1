import logging
import structlog
import sys

# 從集中的設定模組導入日誌級別
from crawler.config import LOG_LEVEL

def configure_logging():
    """
    配置應用程式的日誌系統，整合 structlog 和標準 logging。
    日誌級別從 crawler.config 獲取。
    """
    # 映射日誌級別字串到 logging 模組的常數
    log_level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    numeric_log_level = log_level_map.get(LOG_LEVEL, logging.INFO)

    # 1. 配置 structlog 的處理器鏈
    #    - add_logger_name: 添加 logger 名稱
    #    - add_log_level: 添加日誌級別
    #    - ProcessorFormatter.wrap_for_formatter: 讓 structlog 的事件能被標準 logging 的 formatter 處理
    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # 2. 配置標準 logging 的 formatter 和 handler
    #    - 使用 structlog.dev.ConsoleRenderer 讓日誌在控制台輸出時更美觀
    #    - foreign_pre_chain 確保來自標準 logging 的日誌也能被 structlog 的處理器處理
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(),
        foreign_pre_chain=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
        ],
        # 這裡的 logger_factory 和 wrapper_class 應該被移除，因為它們不屬於 Formatter 的參數
        # logger_factory=structlog.stdlib.LoggerFactory(),
        # wrapper_class=structlog.stdlib.BoundLogger,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    # 3. 配置 root logger
    #    - 移除 logging.basicConfig，避免重複配置
    #    - 設定 root logger 的級別為從 config 讀取的值
    #    - 添加 structlog 處理後的 handler
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(numeric_log_level)

    logger = structlog.get_logger(__name__)
    logger.info("日誌系統配置完成", level=LOG_LEVEL)