# crawler/finmind/config.py
import os
import configparser
import structlog

logger = structlog.get_logger(__name__)

# 讀取專案根目錄下的 local.ini
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), "..", "..", "local.ini")

try:
    config.read(config_path)
except Exception as e:
    logger.critical(f"無法讀取 local.ini 設定檔: {e}", exc_info=True)
    raise RuntimeError("無法讀取設定檔。") from e

# 根據 APP_ENV 選擇區塊
app_env = os.environ.get("APP_ENV", "DOCKER").upper()
if app_env not in config:
    logger.warning(
        f"環境變數 APP_ENV={app_env} 無效或未找到對應區塊，預設使用 [DOCKER] 設定。"
    )
    app_env = "DOCKER"

config_section = config[app_env]

# FinMind 相關設定
FINMIND_API_BASE_URL = config_section.get(
    "FINMIND_API_BASE_URL", "https://api.finmindtrade.com/api/v4/data"
)
FINMIND_START_DATE = config_section.get("FINMIND_START_DATE", "2024-01-01")
FINMIND_END_DATE = config_section.get("FINMIND_END_DATE", "2025-06-17")
