import requests
from requests.exceptions import HTTPError, JSONDecodeError
import structlog
from crawler.worker import app
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import re

from crawler.database.models import Job, Url, SourcePlatform, JobStatus, JobType, SalaryType, CrawlStatus
from crawler.database.connection import get_engine

logger = structlog.get_logger(__name__)

def get_db_session():
    """獲取資料庫 session"""
    engine = get_engine()
    return Session(bind=engine)

def _parse_salary(salary_text: str) -> tuple[int | None, int | None, SalaryType | None]:
    salary_min, salary_max, salary_type = None, None, None

    if "月薪" in salary_text:
        salary_type = SalaryType.MONTHLY
    elif "年薪" in salary_text:
        salary_type = SalaryType.YEARLY
    elif "時薪" in salary_text:
        salary_type = SalaryType.HOURLY
    elif "日薪" in salary_text:
        salary_type = SalaryType.DAILY

    numbers = re.findall(r'(\d+,\d+|\d+)', salary_text)
    numbers = [int(n.replace(',', '')) for n in numbers]

    if len(numbers) == 1:
        salary_min = numbers[0]
        salary_max = numbers[0]
    elif len(numbers) >= 2:
        salary_min = numbers[0]
        salary_max = numbers[1]

    return salary_min, salary_max, salary_type

def _parse_job_type(work_type_text: str) -> JobType | None:
    if "全職" in work_type_text:
        return JobType.FULL_TIME
    elif "兼職" in work_type_text:
        return JobType.PART_TIME
    elif "約聘" in work_type_text:
        return JobType.CONTRACT
    elif "實習" in work_type_text:
        return JobType.INTERNSHIP
    return None

def _parse_posted_date(date_text: str) -> datetime | None:
    try:
        # Assuming date_text is in 'YYYY/MM/DD' format
        return datetime.strptime(date_text, '%Y/%m/%d')
    except ValueError:
        return None

def _save_job_to_db(job_data: dict, original_url: str):
    with get_db_session() as session:
        try:
            # Handle Url table
            url_entry = session.query(Url).filter_by(source_url=original_url).first()
            if url_entry:
                url_entry.details_crawl_status = CrawlStatus.CRAWLED
                url_entry.details_crawled_at = datetime.utcnow()
                url_entry.updated_at = datetime.utcnow()
                logger.info("更新 URL 狀態", url=original_url)
            else:
                url_entry = Url(
                    source_url=original_url,
                    source=SourcePlatform.PLATFORM_104,
                    status=JobStatus.ACTIVE,
                    details_crawl_status=CrawlStatus.CRAWLED,
                    crawled_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                    details_crawled_at=datetime.utcnow()
                )
                session.add(url_entry)
                logger.info("新增 URL", url=original_url)
            
            # Handle Job table
            salary_min, salary_max, salary_type = _parse_salary(job_data.get('salary', ''))
            job_type = _parse_job_type(job_data.get('work_type', ''))
            posted_at = _parse_posted_date(job_data.get('update_date', ''))

            job_entry = session.query(Job).filter_by(
                source_platform=SourcePlatform.PLATFORM_104,
                source_job_id=job_data['job_id']
            ).first()

            if job_entry:
                # Update existing job
                job_entry.url = original_url
                job_entry.status = JobStatus.ACTIVE # Assuming active if successfully crawled
                job_entry.title = job_data.get('title')
                job_entry.description = job_data.get('description')
                job_entry.job_type = job_type
                job_entry.location_text = job_data.get('location')
                job_entry.posted_at = posted_at
                job_entry.salary_text = job_data.get('salary')
                job_entry.salary_min = salary_min
                job_entry.salary_max = salary_max
                job_entry.salary_type = salary_type
                job_entry.experience_required_text = job_data.get('working_experience')
                job_entry.education_required_text = job_data.get('degree')
                job_entry.company_source_id = job_data.get('company_id')
                job_entry.company_name = job_data.get('company_name')
                # job_entry.company_url = job_data.get('company_url') # Not available in extracted_info
                job_entry.updated_at = datetime.utcnow()
                logger.info("更新職缺資訊", job_id=job_data['job_id'])
            else:
                # Create new job
                new_job = Job(
                    source_platform=SourcePlatform.PLATFORM_104,
                    source_job_id=job_data['job_id'],
                    url=original_url,
                    status=JobStatus.ACTIVE,
                    title=job_data.get('title'),
                    description=job_data.get('description'),
                    job_type=job_type,
                    location_text=job_data.get('location'),
                    posted_at=posted_at,
                    salary_text=job_data.get('salary'),
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_type=salary_type,
                    experience_required_text=job_data.get('working_experience'),
                    education_required_text=job_data.get('degree'),
                    company_source_id=job_data.get('company_id'),
                    company_name=job_data.get('company_name'),
                    # company_url=job_data.get('company_url'), # Not available in extracted_info
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                session.add(new_job)
                logger.info("新增職缺資訊", job_id=job_data['job_id'])
            
            session.commit()
            logger.info("職缺資料已成功儲存到資料庫")
        except IntegrityError as e:
            session.rollback()
            logger.error("資料庫完整性錯誤，可能重複", error=e, job_id=job_data['job_id'])
        except Exception as e:
            session.rollback()
            logger.error("儲存職缺資料到資料庫時發生錯誤", error=e, job_id=job_data['job_id'])

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def fetch_104_data(url):
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
        logger.info("職缺內容不存在或已關閉")
        return {}

    extracted_info = {
        'job_id': job_id,
        'update_date': job_data.get('header', {}).get('appearDate'),
        'title': job_data.get('header', {}).get('jobName'),
        'description': job_data.get('jobDetail', {}).get('jobDescription'),
        'salary': job_data.get('jobDetail', {}).get('salary'),
        'work_type': job_data.get('jobDetail', {}).get('workType'),
        'work_time': job_data.get('jobDetail', {}).get('workPeriod'),
        'location': job_data.get('jobDetail', {}).get('addressRegion'),
        'degree': job_data.get('condition', {}).get('edu'),
        'department': job_data.get('jobDetail', {}).get('department'),
        'working_experience': job_data.get('condition', {}).get('workExp'),
        'qualification_required': job_data.get('condition', {}).get('other'),
        'qualification_bonus': job_data.get('welfare', {}).get('welfare'),
        'company_id': job_data.get('header', {}).get('custNo'),
        'company_name': job_data.get('header', {}).get('custName'),
        'company_address': job_data.get('company', {}).get('address'),
        'contact_person': job_data.get('contact', {}).get('hrName'),
        'contact_phone': job_data.get('contact', {}).get('email', '未提供')
    }

    logger.info("提取的職缺資訊", extracted_info=extracted_info)
    _save_job_to_db(extracted_info, url)
    return extracted_info