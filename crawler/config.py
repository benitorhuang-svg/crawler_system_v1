import os
import configparser
import structlog

logger = structlog.get_logger(__name__)

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), "..", "local.ini")

try:
    config.read(config_path)
except Exception as e:
    logger.critical(f"無法讀取 local.ini 設定檔: {e}", exc_info=True)
    raise RuntimeError("無法讀取設定檔。") from e

# Determine which section to use based on APP_ENV environment variable
# Default to 'DOCKER' if APP_ENV is not set or invalid
app_env = os.environ.get("APP_ENV", "DOCKER").upper()
if app_env not in config:
    logger.warning(
        f"環境變數 APP_ENV={app_env} 無效或未找到對應區塊，預設使用 [DOCKER] 設定。"
    )
    app_env = "DOCKER"

config_section = config[app_env]

WORKER_ACCOUNT = config_section.get("WORKER_ACCOUNT", "worker")
WORKER_PASSWORD = config_section.get("WORKER_PASSWORD", "worker")

RABBITMQ_HOST = config_section.get("RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_PORT = int(config_section.get("RABBITMQ_PORT", "5672"))

MYSQL_HOST = config_section.get("MYSQL_HOST", "crawler_jobs_mysql")
MYSQL_PORT = int(config_section.get("MYSQL_PORT", "3306"))
MYSQL_ACCOUNT = config_section.get("MYSQL_ACCOUNT", "root")
MYSQL_ROOT_PASSWORD = config_section.get("MYSQL_ROOT_PASSWORD", "root_password")
MYSQL_PASSWORD = config_section.get("MYSQL_PASSWORD", "root_password")
MYSQL_DATABASE = os.environ.get('CRAWLER_DB_NAME') or config_section.get("MYSQL_DATABASE", "crawler_db")
LOG_LEVEL = config_section.get("LOG_LEVEL", "DEBUG").upper()
LOG_FORMATTER = config_section.get("LOG_FORMATTER", "console").lower()

PRODUCER_BATCH_SIZE = int(config_section.get("PRODUCER_BATCH_SIZE", "100"))
PRODUCER_DISPATCH_INTERVAL_SECONDS = float(
    config_section.get("PRODUCER_DISPATCH_INTERVAL_SECONDS", "1.0")
)

URL_CRAWLER_REQUEST_TIMEOUT_SECONDS = int(
    config_section.get("URL_CRAWLER_REQUEST_TIMEOUT_SECONDS", "30")
)
URL_CRAWLER_UPLOAD_BATCH_SIZE = int(
    config_section.get("URL_CRAWLER_UPLOAD_BATCH_SIZE", "30")
)
URL_CRAWLER_SLEEP_MIN_SECONDS = float(
    config_section.get("URL_CRAWLER_SLEEP_MIN_SECONDS", "0.5")
)
URL_CRAWLER_SLEEP_MAX_SECONDS = float(
    config_section.get("URL_CRAWLER_SLEEP_MAX_SECONDS", "1.5")
)
URL_CRAWLER_API_RETRIES = int(
    config_section.get("URL_CRAWLER_API_RETRIES", "3")
)
URL_CRAWLER_API_BACKOFF_FACTOR = float(
    config_section.get("URL_CRAWLER_API_BACKOFF_FACTOR", "0.5")
)

GEOCODING_RETRY_FAILED_DURATION_HOURS = int(config_section.get("GEOCODING_RETRY_FAILED_DURATION_HOURS", "2"))

def get_db_name_for_platform(platform_enum_value: str) -> str:
    """
    Derives the database name from a SourcePlatform enum value.
    e.g., "platform_104" -> "db_104"
    """
    # Remove "platform_" prefix and add "db_" prefix
    return "db_" + platform_enum_value.replace("platform_", "")
