from crawler.project_104.task_104_jobs import fetch_104_data
import structlog

logger = structlog.get_logger(__name__)

job_url = "https://www.104.com.tw/job/7anso"
fetch_104 = fetch_104_data.s(job_url)
fetch_104.apply_async() 
logger.info("send task_104 task")

