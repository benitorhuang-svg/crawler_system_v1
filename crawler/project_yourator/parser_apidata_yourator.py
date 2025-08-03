from datetime import datetime
from typing import Optional, Dict, Any

import structlog

from crawler.database.schemas import (
    JobPydantic,
    SourcePlatform,
    JobStatus,
    JobType,
)
from crawler.utils.salary_parser import parse_salary_text

logger = structlog.get_logger(__name__)

JOB_TYPE_MAPPING_YOURATOR = {
    "full_time": JobType.FULL_TIME,
    "part_time": JobType.PART_TIME,
    "internship": JobType.INTERNSHIP,
    "contract": JobType.CONTRACT,
}


def parse_job_detail_to_pydantic(job_data: Dict[str, Any]) -> Optional[JobPydantic]:
    """
    從 Yourator 單一職缺 API 的 JSON 數據解析並轉換為 JobPydantic 物件。
    """
    try:
        source_job_id = str(job_data.get("id"))
        company_source_id = str(job_data.get("company", {}).get("id"))

        url = f"https://www.yourator.co/jobs/{source_job_id}"
        company_url = f"https://www.yourator.co/companies/{company_source_id}"

        title = job_data.get("name")
        company_name = job_data.get("company", {}).get("name")
        description = job_data.get("description")
        location_text = ", ".join(job_data.get("locations", []))

        posted_at = None
        created_at_str = job_data.get("created_at")
        if created_at_str:
            try:
                posted_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            except ValueError:
                logger.warning(
                    "Could not parse posted_at date format from detail API.",
                    created_at=created_at_str,
                    job_id=source_job_id,
                )

        salary_text = f"{job_data.get('salary_min')} - {job_data.get('salary_max')} {job_data.get('salary_type')}"
        salary_min, salary_max, salary_type = parse_salary_text(salary_text)

        job_type_str = job_data.get("job_type")
        job_type = JOB_TYPE_MAPPING_YOURATOR.get(job_type_str, JobType.OTHER)

        experience_required_text = job_data.get("year_of_experience")
        education_required_text = "不拘"

        return JobPydantic(
            source_platform=SourcePlatform.PLATFORM_YOURATOR,
            source_job_id=source_job_id,
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
            company_source_id=company_source_id,
            company_name=company_name,
            company_url=company_url,
        )

    except Exception as e:
        logger.error(
            "Unexpected error when parsing Yourator job detail JSON.",
            error=e,
            job_data=job_data,
            exc_info=True,
        )
        return None

def parse_job_list_to_pydantic(job_item: Dict[str, Any]) -> Optional[JobPydantic]:
    """
    從 Yourator 列表頁 API 的 JSON 數據解析並轉換為 JobPydantic 物件。
    """
    try:
        source_job_id = str(job_item.get("id"))
        company_source_id = str(job_item.get("company", {}).get("id"))

        url = f"https://www.yourator.co/jobs/{source_job_id}"
        company_url = f"https://www.yourator.co/companies/{company_source_id}"

        title = job_item.get("name")
        company_name = job_item.get("company", {}).get("name")
        description = job_item.get("description")
        location_text = ", ".join(job_item.get("locations", []))

        posted_at = None
        created_at_str = job_item.get("created_at")
        if created_at_str:
            try:
                posted_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            except ValueError:
                logger.warning(
                    "Could not parse posted_at date format from list API.",
                    created_at=created_at_str,
                    job_id=source_job_id,
                )

        salary_text = f"{job_item.get('salary_min')} - {job_item.get('salary_max')} {job_item.get('salary_type')}"
        salary_min, salary_max, salary_type = parse_salary_text(salary_text)

        job_type_str = job_item.get("job_type")
        job_type = JOB_TYPE_MAPPING_YOURATOR.get(job_type_str, JobType.OTHER)

        experience_required_text = job_item.get("year_of_experience")
        education_required_text = "不拘"

        return JobPydantic(
            source_platform=SourcePlatform.PLATFORM_YOURATOR,
            source_job_id=source_job_id,
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
            company_source_id=company_source_id,
            company_name=company_name,
            company_url=company_url,
        )

    except Exception as e:
        logger.error(
            "Unexpected error when parsing Yourator job list JSON.",
            error=e,
            job_item=job_item,
            exc_info=True,
        )
        return None
