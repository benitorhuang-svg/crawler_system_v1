from celery import Celery
import structlog
import os
from dotenv import load_dotenv

from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger()

load_dotenv()

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST")
RABBITMQ_PORT = os.environ.get("RABBITMQ_PORT")
WORKER_ACCOUNT = os.environ.get("WORKER_ACCOUNT")
WORKER_PASSWORD = os.environ.get("WORKER_PASSWORD")

logger.info(
    "RabbitMQ configuration",
    rabbitmq_host=RABBITMQ_HOST,
    rabbitmq_port=RABBITMQ_PORT,
    worker_account=WORKER_ACCOUNT,
    worker_password=WORKER_PASSWORD,
)
app = Celery(
    "task",
    # 只包含 tasks.py 裡面的程式, 才會成功執行
    include=[
        "crawler.project_104.task_104_jobs",

    ],
    # 連線到 rabbitmq
    broker=f"pyamqp://{WORKER_ACCOUNT}:{WORKER_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/",
    task_routes={
        'crawler.project_104.task_104_jobs.fetch_104_data': {'queue': 'jobs_104'},
        # 如果有其他任務，可以在這裡添加更多路由
    }
)
