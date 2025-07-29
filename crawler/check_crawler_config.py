import structlog

from .logging_config import configure_logging
from .config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
)

configure_logging()

logger = structlog.get_logger(__name__) # Corrected: add __name__

logger.info(
    "RabbitMQ configuration check.", # Improved log message
    rabbitmq_host=RABBITMQ_HOST,
    rabbitmq_port=RABBITMQ_PORT,
    worker_account="***masked***", # Masked sensitive info
    worker_password="***masked***", # Masked sensitive info
)