from celery import Celery
import structlog
import logging # Import logging module
import sys # Import sys module

from crawler.config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    WORKER_ACCOUNT,
    WORKER_PASSWORD,
    LOG_LEVEL, # Import LOG_LEVEL
)

# configure_logging() # Removed direct call
logger = structlog.get_logger(__name__)

logger.info(
    "RabbitMQ configuration",
    rabbitmq_host=RABBITMQ_HOST,
    rabbitmq_port=RABBITMQ_PORT,
    worker_account="***masked***",
    worker_password="***masked***",
)

app = Celery(
    "task",
    include=[
        "crawler.project_104.task_category_104",
        "crawler.project_104.task_jobs_104",
        "crawler.project_104.task_urls_104",
        "crawler.project_cakeresume.task_category_cakeresume",
        "crawler.project_cakeresume.task_jobs_cakeresume",
        "crawler.project_cakeresume.task_urls_cakeresume",
        "crawler.project_1111.task_category_1111",
        "crawler.project_1111.task_jobs_1111",
        "crawler.project_1111.task_urls_1111",
        "crawler.project_yes123.task_category_yes123",
        "crawler.project_yes123.task_jobs_yes123",
        "crawler.project_yes123.task_urls_yes123",
    ],
    # Configure broker connection settings for robustness
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=10,
    broker_connection_timeout=30,
)

# Set the broker URL using app.conf
app.conf.broker_url = (
    f"pyamqp://{WORKER_ACCOUNT}:{WORKER_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
)

# Initialize database when Celery app is ready
@app.on_after_configure.connect
def setup_database_connection(sender, **kwargs):
    from crawler.database.connection import initialize_database
    initialize_database()
    logger.info("Celery app configured and database initialized.")

# Configure Celery's logging to use structlog
@app.on_after_configure.connect
def setup_logging(sender, **kwargs):
    # 確保只配置一次
    if not logging.getLogger().handlers:
        # 配置 structlog 的處理器鏈
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

        # 配置標準 logging 的 formatter 和 handler
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(level=getattr(logging, LOG_LEVEL, logging.INFO)),
            foreign_pre_chain=[
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
            ],
        )

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO)) # Set handler level

        # 配置 root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

        # 確保 Celery 自己的日誌也通過 structlog 處理
        logging.getLogger('celery').addHandler(handler)
        logging.getLogger('celery').setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

        logger.info("Celery 日誌系統配置完成", configured_level=LOG_LEVEL)

app.conf.task_routes = {
    "crawler.project_104.task_jobs_104.fetch_url_data_104": {"queue": "producer_jobs_104"},
    "crawler.project_104.task_urls_104.crawl_and_store_category_urls": {
        "queue": "producer_urls_104"
    },
    "crawler.project_104.task_category_104.fetch_url_data_104": {"queue": "producer_category_104"},
    "crawler.project_cakeresume.task_jobs_cakeresume.fetch_url_data_cakeresume": {"queue": "producer_jobs_cakeresume"},
    "crawler.project_cakeresume.task_urls_cakeresume.crawl_and_store_cakeresume_category_urls": {
        "queue": "producer_urls_cakeresume"
    },
    "crawler.project_cakeresume.task_category_cakeresume.fetch_url_data_cakeresume": {"queue": "producer_category_cakeresume"},
    "crawler.project_1111.task_jobs_1111.fetch_url_data_1111": {"queue": "producer_jobs_1111"},
    "crawler.project_1111.task_urls_1111.crawl_and_store_1111_category_urls": {
        "queue": "producer_urls_1111"
    },
    "crawler.project_1111.task_category_1111.fetch_url_data_1111": {"queue": "producer_category_1111"},
    "crawler.project_yes123.task_jobs_yes123.fetch_url_data_yes123": {"queue": "producer_jobs_yes123"},
    "crawler.project_yes123.task_urls_yes123.crawl_and_store_yes123_category_urls": {
        "queue": "producer_urls_yes123"
    },
    "crawler.project_yes123.task_category_yes123.fetch_url_data_yes123": {"queue": "producer_category_yes123"},
}
