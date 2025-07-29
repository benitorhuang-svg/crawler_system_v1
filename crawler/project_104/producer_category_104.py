from .task_category_104 import fetch_url_data_104
import structlog

logger = structlog.get_logger(__name__)

JobCat_url_104 = "https://static.104.com.tw/category-tool/json/JobCat.json"

fetch_url_data_104.delay(JobCat_url_104)
logger.info("send task_category_104 url")