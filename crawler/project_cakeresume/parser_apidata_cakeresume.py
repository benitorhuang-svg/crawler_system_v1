import re
from datetime import datetime, timezone
from typing import Optional
import structlog

from bs4 import BeautifulSoup

from crawler.database.models import (
    JobPydantic,
    JobStatus,
    JobType,
    SalaryType,
    SourcePlatform,
)

logger = structlog.get_logger(__name__)

# CakeResume 的 job_type 到我們內部 JobType Enum 的映射
JOB_TYPE_MAPPING_CAKERESUME = {
    "full-time": JobType.FULL_TIME,
    "part-time": JobType.PART_TIME,
    "internship": JobType.INTERNSHIP,
    "contract": JobType.CONTRACT,
    "other": JobType.OTHER, # Added for cases where job_type is None
    # 根據實際資料補充更多映射
}

# CakeResume 的 salary_type 到我們內部 SalaryType Enum 的映射
SALARY_TYPE_MAPPING_CAKERESUME = {
    "monthly": SalaryType.MONTHLY,
    "yearly": SalaryType.YEARLY,
    "hourly": SalaryType.HOURLY,
    "daily": SalaryType.DAILY,
    # CakeResume 沒有明確的 "by case" 或 "negotiable"，需要根據 min/max 和 hide 欄位判斷
}

def clean_html_if_string(value):
    """
    輔助函數：只在輸入值為字串時，才清除 HTML 標籤。
    對於其他類型（數字、列表、None 等），直接返回原值。
    """
    if isinstance(value, str):
        return BeautifulSoup(value, "html.parser").get_text(separator=' ', strip=True)
    return value


def parse_cakeresume_job_data_to_pydantic(job_details: dict, url: str) -> Optional[JobPydantic]:
    """
    從 CakeResume 的 job_details 字典解析並轉換為 JobPydantic 物件。
    """
    try:
        # CakeResume 的 job_id 通常是 URL 的最後一部分，例如 /jobs/2f3db0 中的 2f3db0
        job_id_match = re.search(r'/jobs/([a-zA-Z0-9]+)', url)
        job_id = job_id_match.group(1) if job_id_match else None

        if not job_id:
            logger.error("Failed to extract job_id from URL for parsing.", url=url)
            return None

        # Extract data from job_details dictionary
        title = job_details.get('title')
        description = clean_html_if_string(job_details.get('description'))
        # 補充 description: 如果 description 為空，嘗試從 job_details.get('job_responsibilities') 或 job_details.get('requirements') 獲取
        if not description:
            description = clean_html_if_string(job_details.get('job_responsibilities'))
        if not description:
            description = clean_html_if_string(job_details.get('requirements'))

        job_type_raw = job_details.get('job_type')
        job_type = JOB_TYPE_MAPPING_CAKERESUME.get(job_type_raw)
        if job_type is None:
            job_type = JobType.OTHER # Default to OTHER if not mapped

        locations = job_details.get('locations', [])
        location_text = ', '.join(loc.get('name') for loc in locations if loc.get('name')) if locations else None

        posted_at = None
        created_at_str = job_details.get('created_at')
        if created_at_str:
            try:
                # CakeResume uses ISO format with Z for UTC
                posted_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')).astimezone(timezone.utc).replace(tzinfo=None)
            except ValueError:
                logger.warning("Could not parse posted_at date format.", created_at=created_at_str, job_id=job_id)

        salary_min = job_details.get('salary_min')
        salary_max = job_details.get('salary_max')
        salary_type_raw = job_details.get('salary_type')
        salary_type = SALARY_TYPE_MAPPING_CAKERESUME.get(salary_type_raw)
        salary_currency = job_details.get('salary_currency')
        salary_text = f"{salary_currency} {salary_min}-{salary_max} ({salary_type_raw})" if salary_min and salary_max else None
        if job_details.get('hide_salary_completely'):
            salary_text = "面議"
            salary_type = SalaryType.NEGOTIABLE
            salary_min = None
            salary_max = None

        experience_required_text = job_details.get('seniority_level') # e.g., "Entry", "Mid", "Senior"
        min_work_exp_year = job_details.get('min_work_exp_year')
        if min_work_exp_year is not None:
            experience_required_text = f"{min_work_exp_year} 年以上" if min_work_exp_year > 0 else "不拘"
        
        # 將 None 轉換為 "不拘"
        if experience_required_text is None:
            experience_required_text = "不拘"

        education_required_text = None # CakeResume data doesn't seem to have a direct field for education
        if education_required_text is None:
            education_required_text = "不拘"

        company_info = job_details.get('company', {})
        company_source_id = company_info.get('id')
        company_name = company_info.get('name')
        company_url = company_info.get('website_url')

        job_pydantic_data = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_CAKERESUME,
            source_job_id=job_id,
            url=url,
            status=JobStatus.ACTIVE,
            title=title,
            description=description,
            job_type=job_type,
            location_text=location_text,
            posted_at=posted_at,
            salary_text=salary_text,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
            experience_required_text=experience_required_text,
            education_required_text=education_required_text,
            company_source_id=str(company_source_id) if company_source_id else None,
            company_name=company_name,
            company_url=company_url,
        )
        return job_pydantic_data

    except Exception as e:
        logger.error(
            "Unexpected error when parsing CakeResume job data to Pydantic.",
            error=e,
            url=url,
            exc_info=True,
        )
        return None
