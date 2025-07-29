import requests
from requests.exceptions import HTTPError, JSONDecodeError
import structlog
from crawler.worker import app
from crawler.database.models import SourcePlatform, JobPydantic, JobStatus
from crawler.database.repository import upsert_jobs

logger = structlog.get_logger(__name__)

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def fetch_url_data_104(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'referer': 'https://www.104.com.tw/'
    }

    job_id = url.split('/')[-1].split('?')[0]
    url_api = f'https://www.104.com.tw/job/ajax/content/{job_id}'
    
    try:
        response = requests.get(url_api, headers=headers)
        response.raise_for_status()
        data = response.json()
    except (HTTPError, JSONDecodeError) as err:
        logger.error("發生錯誤", error=err)
        return {}
    
    job_data = data.get('data', {})
    if not job_data or job_data.get('custSwitch', {}) == "off":
        logger.info("職缺內容不存在或已關閉", url=url)
        return {}

    # 提取並轉換資料為 JobPydantic 格式
    try:
        job_pydantic_data = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_job_id=job_id,
            url=url,
            status=JobStatus.ACTIVE, # 預設為啟用狀態
            title=job_data.get('header', {}).get('jobName', ''),
            description=job_data.get('jobDetail', {}).get('jobDescription', ''),
            job_type=None, # 根據實際資料調整
            location_text=job_data.get('jobDetail', {}).get('addressRegion', ''),
            posted_at=None, # 需要日期格式轉換
            salary_text=job_data.get('jobDetail', {}).get('salary', ''),
            salary_min=None, # 需要從 salary_text 解析
            salary_max=None, # 需要從 salary_text 解析
            salary_type=None, # 需要從 salary_text 解析
            experience_required_text=job_data.get('condition', {}).get('workExp', ''),
            education_required_text=job_data.get('condition', {}).get('edu', ''),
            company_source_id=job_data.get('header', {}).get('custNo', ''),
            company_name=job_data.get('header', {}).get('custName', ''),
            company_url=job_data.get('company', {}).get('address', ''), # 這裡可能需要調整，company.address 可能是地址而不是 URL
        )
        upsert_jobs([job_pydantic_data])
        logger.info("Successfully extracted and upserted job information.", job_id=job_id)
        return job_pydantic_data.model_dump()
    except Exception as e:
        logger.error(f"Error processing job data for {job_id}: {e}", exc_info=True)
        return {}


if __name__ == "__main__":
    # This block is for local testing purposes only.
    # In a real Celery setup, this task would be invoked by a worker.
    sample_job_url = "https://www.104.com.tw/job/7anso"
    fetch_url_data_104(sample_job_url)