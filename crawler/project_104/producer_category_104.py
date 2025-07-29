from .task_category_104 import fetch_url_data_104
import structlog
import time # Added import time

from crawler.logging_config import configure_logging
from crawler.project_104.config_104 import JOB_CAT_URL_104 # Changed import path

configure_logging()
logger = structlog.get_logger(__name__)

time.sleep(5) # Added a 5-second delay
fetch_url_data_104.delay(JOB_CAT_URL_104)
logger.info("send task_category_104 url", url=JOB_CAT_URL_104)