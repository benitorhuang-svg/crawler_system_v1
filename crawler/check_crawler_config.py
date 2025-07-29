


import structlog

from .logging_config import configure_logging

configure_logging()
logger = structlog.get_logger()

from .config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    WORKER_ACCOUNT,
    WORKER_PASSWORD,
)


logger.info(
    "RabbitMQ configuration",
    rabbitmq_host=RABBITMQ_HOST,
    rabbitmq_port=RABBITMQ_PORT,
    worker_account=WORKER_ACCOUNT,
    worker_password=WORKER_PASSWORD,
)
