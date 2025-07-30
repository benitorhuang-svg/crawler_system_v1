import logging
import structlog
import sys

# 從集中的設定模組導入日誌級別和格式化工具
from crawler.config import LOG_LEVEL, LOG_FORMATTER


def configure_logging():
    """
    配置應用程式的日誌系統，整合 structlog 和標準 logging。
    日誌級別和格式化工具從 crawler.config 獲取。
    """
    numeric_log_level = getattr(logging, LOG_LEVEL, logging.INFO)

    # 檢查是否已經配置過，避免重複添加 handler
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        # 1. 配置 structlog 的處理器鏈
        #    - TimeStamper: 添加時間戳
        #    - add_logger_name, add_log_level: 添加日誌器名稱和級別
        #    - wrap_for_formatter: 為標準庫的 formatter 準備
        processors = [
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ]
        structlog.configure(
            processors=processors,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

        # 2. 根據 LOG_FORMATTER 選擇渲染器
        #    - console: 美觀、適合開發但效能較差
        #    - key_value: 結構化、易讀且效能好
        #    - json: 機器可讀、效能最佳，適合生產環境
        if LOG_FORMATTER == "console":
            renderer = structlog.dev.ConsoleRenderer()
        elif LOG_FORMATTER == "key_value":
            renderer = structlog.processors.KeyValueRenderer(key_order=['timestamp', 'level', 'event'])
        else:
            renderer = structlog.processors.JSONRenderer()

        # 3. 配置標準 logging 的 formatter 和 handler
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=renderer,
            foreign_pre_chain=[
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
            ],
        )

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)

        # 4. 配置 root logger
        root_logger.addHandler(handler)
        root_logger.setLevel(numeric_log_level)

        logger = structlog.get_logger(__name__)
        logger.info(
            "日誌系統配置完成",
            configured_level=LOG_LEVEL,
            formatter=LOG_FORMATTER,
        )
