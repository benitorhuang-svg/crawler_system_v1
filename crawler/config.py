import os
import configparser
import structlog

logger = structlog.get_logger(__name__)

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', 'local.ini')

try:
    config.read(config_path)
except Exception as e:
    logger.critical(f"無法讀取 local.ini 設定檔: {e}", exc_info=True)
    raise RuntimeError("無法讀取設定檔。") from e

# Determine which section to use based on APP_ENV environment variable
# Default to 'DOCKER' if APP_ENV is not set or invalid
app_env = os.environ.get("APP_ENV", "DOCKER").upper()
if app_env not in config:
    logger.warning(f"環境變數 APP_ENV={app_env} 無效或未找到對應區塊，預設使用 [DOCKER] 設定。")
    app_env = "DOCKER"

config_section = config[app_env]

WORKER_ACCOUNT = config_section.get("WORKER_ACCOUNT")
WORKER_PASSWORD = config_section.get("WORKER_PASSWORD")

RABBITMQ_HOST = config_section.get("RABBITMQ_HOST")
RABBITMQ_PORT = int(config_section.get("RABBITMQ_PORT"))

MYSQL_HOST = config_section.get("MYSQL_HOST")
MYSQL_PORT = int(config_section.get("MYSQL_PORT"))
MYSQL_ACCOUNT = config_section.get("MYSQL_ACCOUNT")
MYSQL_ROOT_PASSWORD = config_section.get("MYSQL_ROOT_PASSWORD")
MYSQL_PASSWORD = config_section.get("MYSQL_PASSWORD")
MYSQL_DATABASE = config_section.get("MYSQL_DATABASE")
LOG_LEVEL = config_section.get("LOG_LEVEL", "INFO").upper()

PRODUCER_BATCH_SIZE = int(config_section.get("PRODUCER_BATCH_SIZE", "100"))
PRODUCER_DISPATCH_INTERVAL_SECONDS = float(config_section.get("PRODUCER_DISPATCH_INTERVAL_SECONDS", "1.0"))

URL_CRAWLER_REQUEST_TIMEOUT_SECONDS = int(config_section.get("URL_CRAWLER_REQUEST_TIMEOUT_SECONDS", "20"))
URL_CRAWLER_UPLOAD_BATCH_SIZE = int(config_section.get("URL_CRAWLER_UPLOAD_BATCH_SIZE", "30"))
URL_CRAWLER_SLEEP_MIN_SECONDS = float(config_section.get("URL_CRAWLER_SLEEP_MIN_SECONDS", "0.5"))
URL_CRAWLER_SLEEP_MAX_SECONDS = float(config_section.get("URL_CRAWLER_SLEEP_MAX_SECONDS", "1.5"))
