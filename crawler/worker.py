from celery import Celery
import structlog

from crawler.logging_config import configure_logging
from crawler.config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    WORKER_ACCOUNT,
    WORKER_PASSWORD,
)

configure_logging()
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

app.conf.task_routes = {
    "crawler.project_104.task_jobs_104.fetch_url_data_104": {"queue": "jobs_104"},
    "crawler.project_104.task_urls_104.crawl_and_store_category_urls": {
        "queue": "urls_104"
    },
    "crawler.project_104.task_category_104.fetch_url_data_104": {"queue": "category_104"},
}
