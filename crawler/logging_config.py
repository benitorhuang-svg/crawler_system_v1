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
    numeric_log_level = getattr(logging, LOG_LEVEL, logging.INFO)

    # 檢查是否已經配置過，避免重複添加 handler
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        # 1. 配置 structlog 的處理器鏈
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
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(),
            foreign_pre_chain=[
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
            ],
        )

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)

        # 3. 配置 root logger
        root_logger.addHandler(handler)
        root_logger.setLevel(numeric_log_level)

        logger = structlog.get_logger(__name__)
        logger.info("日誌系統配置完成", configured_level=LOG_LEVEL)
