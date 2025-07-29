from crawler.project_104.task_jobs_104 import fetch_url_data_104
import structlog

logger = structlog.get_logger(__name__)

category_url_104 = "https://www.104.com.tw/job/7anso"
category_url_104 = fetch_url_data_104.delay(category_url_104)
logger.info("send task_jobs_104 url")

