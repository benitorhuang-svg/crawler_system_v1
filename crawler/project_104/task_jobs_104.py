import json
import requests
import structlog
from crawler.worker import app
from crawler.database.models import SourcePlatform, JobPydantic, JobStatus
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled

from typing import Optional
import re
from datetime import datetime
from crawler.database.models import SalaryType, CrawlStatus

logger = structlog.get_logger(__name__)

def parse_salary(salary_text: str) -> (Optional[int], Optional[int], Optional[SalaryType]):
    salary_min, salary_max, salary_type = None, None, None
    text = salary_text.replace(",", "").lower()

    # 月薪
    match_monthly = re.search(r'月薪([0-9]+)(?:[至~])([0-9]+)元', text) or re.search(r'月薪([0-9]+)元以上', text)
    if match_monthly:
        salary_type = SalaryType.MONTHLY
        salary_min = int(match_monthly.group(1))
        if match_monthly.group(2) if len(match_monthly.groups()) > 1 else None:
            salary_max = int(match_monthly.group(2))
        return salary_min, salary_max, salary_type

    # 年薪
    match_yearly = re.search(r'年薪([0-9]+)萬(?:[至~])([0-9]+)萬', text) or re.search(r'年薪([0-9]+)萬以上', text)
    if match_yearly:
        salary_type = SalaryType.YEARLY
        salary_min = int(match_yearly.group(1)) * 10000
        if match_yearly.group(2) if len(match_yearly.groups()) > 1 else None:
            salary_max = int(match_yearly.group(2)) * 10000
        return salary_min, salary_max, salary_type

    # 時薪
    match_hourly = re.search(r'時薪([0-9]+)元', text)
    if match_hourly:
        salary_type = SalaryType.HOURLY
        salary_min = int(match_hourly.group(1))
        salary_max = int(match_hourly.group(1))
        return salary_min, salary_max, salary_type

    # 日薪
    match_daily = re.search(r'日薪([0-9]+)元', text)
    if match_daily:
        salary_type = SalaryType.DAILY
        salary_min = int(match_daily.group(1))
        salary_max = int(match_daily.group(1))
        return salary_min, salary_max, salary_type

    # 論件計酬
    if "論件計酬" in text:
        salary_type = SalaryType.BY_CASE
        return None, None, salary_type

    # 面議
    if "面議" in text:
        salary_type = SalaryType.NEGOTIABLE
        return None, None, salary_type

    return salary_min, salary_max, salary_type

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def fetch_url_data_104(url: str) -> Optional[JobPydantic]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Referer': 'https://www.104.com.tw/job/'
    }

    try:
        # 從 URL 中提取 job_id
        job_id = url.split('/')[-1].split('?')[0]
        if not job_id:
            logger.error(f"無法從 URL 中提取 job_id: {url}")
            return None
            
        # 組合 API URL
        api_url = f'https://www.104.com.tw/job/ajax/content/{job_id}'
        
        logger.info(f"正在抓取職缺 ID: {job_id}，來源 URL: {api_url}")
        
        # 發送 HTTP 請求
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status()  # 如果狀態碼不是 2xx，則拋出異常
        
        data = response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"請求 API 時發生網路錯誤: {e}")
        return None
    except json.JSONDecodeError:
        logger.error(f"解析 JSON 時失敗。URL: {api_url}")
        return None

    # --- 資料解析 ---
    # 根據您提供的 JSON 結構進行解析
    job_data = data.get('data')
    if not job_data or job_data.get('switch') == "off":
        logger.warning(f"職缺內容不存在或已關閉。Job ID: {job_id}")
        return None

    try:
        header = job_data.get('header', {})
        job_detail = job_data.get('jobDetail', {})
        condition = job_data.get('condition', {})

        # 組合 location_text
        job_addr_region = job_detail.get('addressRegion', '')
        job_address_detail = job_detail.get('addressDetail', '')
        # 確保只有在兩個欄位都存在時才組合，避免產生不完整的地址
        location_text = (job_addr_region + job_address_detail).strip()
        if not location_text:
            location_text = None

        # 解析 posted_at
        posted_at = None
        appear_date_str = header.get('appearDate')
        if appear_date_str:
            try:
                posted_at = datetime.strptime(appear_date_str, '%Y/%m/%d')
            except ValueError:
                logger.warning("無法解析 posted_at 日期格式", appear_date=appear_date_str)

        # 解析薪資資訊
        salary_min, salary_max, salary_type = parse_salary(job_detail.get('salary', ''))

        job_pydantic_data = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_job_id=job_id,
            url=url,
            status=JobStatus.ACTIVE, # 預設為啟用狀態
            title=header.get('jobName'),
            description=job_detail.get('jobDescription'),
            job_type=job_detail.get('jobType'),
            location_text=location_text,
            posted_at=posted_at, # 需要日期格式轉換
            salary_text=job_detail.get('salary'),
            salary_min=salary_min, # 需要從 salary_text 解析
            salary_max=salary_max, # 需要從 salary_text 解析
            salary_type=salary_type, # 需要從 salary_text 解析
            experience_required_text=condition.get('workExp'),
            education_required_text=condition.get('edu'),
            company_source_id=header.get('custNo'),
            company_name=header.get('custName'),
            company_url=header.get('custUrl'),
        )
        
        upsert_jobs([job_pydantic_data])
        logger.info(f"成功解析職缺: {job_pydantic_data.title}")
        mark_urls_as_crawled({CrawlStatus.COMPLETED: [url]})
        return job_pydantic_data.model_dump()

    except (AttributeError, KeyError) as e:
        logger.error(f"解析資料時遺失關鍵欄位: {e}。Job ID: {job_id}", exc_info=True)
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        # 印出收到的資料，方便除錯
        # logger.debug(json.dumps(job_data, indent=2, ensure_ascii=False))
        return {}
    except Exception as e: # 捕獲其他未預期的錯誤
        logger.error(f"處理職缺資料時發生未預期錯誤: {e}", exc_info=True)
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return {}


# if __name__ == "__main__":
#     # 啟動本地測試 task_jobs_104
#     # APP_ENV=DEV python -m crawler.project_104.task_jobs_104

#     from crawler.database.connection import initialize_database
#     from crawler.database.repository import get_unprocessed_urls
    
#     initialize_database()
#     logger.info("在本地測試 task_jobs_104，從資料庫獲取 10 個 URL...")
    
#     # 從資料庫獲取 10 個未處理的 URL
#     urls_to_test = get_unprocessed_urls(SourcePlatform.PLATFORM_104, 10)
    
#     if urls_to_test:
#         for url_obj in urls_to_test:
#             logger.info("處理測試 URL", url=url_obj.source_url)
#             fetch_url_data_104(url_obj.source_url) # 直接呼叫函數，不使用 delay
#     else:
#         logger.info("資料庫中沒有未處理的 URL 可供測試。請先執行 task_urls_104 填充資料。")