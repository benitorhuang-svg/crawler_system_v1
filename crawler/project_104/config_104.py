# crawler/project_104/config.py
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

# 104 平台相關設定
JOB_CAT_URL_104 = config_section.get(
    "JOB_CAT_URL_104", "https://static.104.com.tw/category-tool/json/JobCat.json"
)
JOB_API_BASE_URL_104 = config_section.get(
    "JOB_API_BASE_URL_104", "https://www.104.com.tw/job/ajax/content/"
)
WEB_NAME_104 = config_section.get("WEB_NAME_104", "104_人力銀行")

HEADERS_104 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Referer": "https://www.104.com.tw/jobs/search",
}

HEADERS_104_JOB_API = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Referer": "https://www.104.com.tw/job/",
}

HEADERS_104_URL_CRAWLER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Referer": "https://www.104.com.tw/",
}

URL_CRAWLER_BASE_URL_104 = config_section.get(
    "URL_CRAWLER_BASE_URL_104", "https://www.104.com.tw/jobs/search/api/jobs"
)
URL_CRAWLER_PAGE_SIZE_104 = int(config_section.get("URL_CRAWLER_PAGE_SIZE_104", "20"))
URL_CRAWLER_ORDER_BY_104 = int(
    config_section.get("URL_CRAWLER_ORDER_BY_104", "16")
)  # 16 (最近更新)
