import requests
import sys
from requests.exceptions import HTTPError, JSONDecodeError
import structlog

from crawler.worker import app
from crawler.logging_config import configure_logging  # Import configure_logging
from crawler.config import JOB_API_BASE_URL_104  # Import the base URL from config

configure_logging()  # Call configure_logging at the beginning
logger = structlog.get_logger(__name__)


# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def get_job_api_data(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "referer": "https://www.104.com.tw/",
    }

    job_id = url.split("/")[-1].split("?")[0]
    # Use the configured base URL
    url_api = f"{JOB_API_BASE_URL_104}{job_id}"

    try:
        response = requests.get(url_api, headers=headers)
        response.raise_for_status()
        data = response.json()
    except (HTTPError, JSONDecodeError) as err:
        logger.error(
            "Failed to fetch job API data", url=url_api, error=err
        )  # Improved log message
        return {}

    job_data = data.get("data", {})
    if not job_data or job_data.get("custSwitch", {}) == "off":
        logger.info(
            "Job content does not exist or is closed", job_id=job_id
        )  # Improved log message
        return {}

    extracted_info = {
        "job_id": job_id,
        "update_date": job_data.get("header", {}).get("appearDate"),
        "title": job_data.get("header", {}).get("jobName"),
        "description": job_data.get("jobDetail", {}).get("jobDescription"),
        "salary": job_data.get("jobDetail", {}).get("salary"),
        "work_type": job_data.get("jobDetail", {}).get("workType"),
        "work_time": job_data.get("jobDetail", {}).get("workPeriod"),
        "location": job_data.get("jobDetail", {}).get("addressRegion"),
        "degree": job_data.get("condition", {}).get("edu"),
        "department": job_data.get("jobDetail", {}).get("department"),
        "working_experience": job_data.get("condition", {}).get("workExp"),
        "qualification_required": job_data.get("condition", {}).get("other"),
        "qualification_bonus": job_data.get("welfare", {}).get("welfare"),
        "company_id": job_data.get("header", {}).get("custNo"),
        "company_name": job_data.get("header", {}).get("custName"),
        "company_address": job_data.get("company", {}).get("address"),
        "contact_person": job_data.get("contact", {}).get("hrName"),
        "contact_phone": job_data.get("contact", {}).get("email", "未提供"),
    }

    logger.info(
        "Extracted job information", job_id=job_id, extracted_info=extracted_info
    )  # Improved log message
    return extracted_info  # Return the extracted info


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.info(
            "Usage: python local_fetch_104_url_data.py <job_url>"
        )  # Updated usage message
        sys.exit(1)

    job_url = sys.argv[1]
    logger.info("Dispatching job API data task", job_url=job_url)
    get_job_api_data.delay(job_url)
