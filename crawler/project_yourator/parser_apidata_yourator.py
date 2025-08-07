import os
from datetime import datetime
from typing import Optional, Dict, Any

import structlog
import pandas as pd

from crawler.database.schemas import (
    JobPydantic,
    SourcePlatform,
    JobStatus,
    JobType,
    LocationPydantic,
    SkillPydantic,
    CompanyPydantic,
)
from crawler.utils.salary_parser import parse_salary_text
from crawler.utils.run_skill_extraction import extract_skills_precise

logger = structlog.get_logger(__name__)

SKILL_MASTER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'utils',
    'skill_data',
    'generated_data',
    'skill_master.json'
)

try:
    SKILL_MASTER_DF = pd.read_json(SKILL_MASTER_PATH)
    logger.info(f"已載入技能主檔: {SKILL_MASTER_PATH}")
except FileNotFoundError:
    logger.error(f"錯誤：找不到技能主檔。請先執行 `python3 -m skill_tool.run_skill_extraction --generate-kb` 來生成 {SKILL_MASTER_PATH}")
    SKILL_MASTER_DF = pd.DataFrame() # Provide an empty DataFrame to avoid errors later
except Exception as e:
    logger.error(f"載入技能主檔失敗: {e}")
    SKILL_MASTER_DF = pd.DataFrame() # Provide an empty DataFrame to avoid errors later

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
                    event="parse_date_format_error",
                    created_at=created_at_str,
                    job_id=source_job_id,
                    platform=SourcePlatform.PLATFORM_YOURATOR,
                    component="parser",
                )

        salary_text = f"{job_data.get('salary_min')} - {job_data.get('salary_max')} {job_data.get('salary_type')}"
        salary_min, salary_max, salary_type = parse_salary_text(salary_text)

        job_type_str = job_data.get("job_type")
        job_type = JOB_TYPE_MAPPING_YOURATOR.get(job_type_str, JobType.OTHER)

        experience_required_text = job_data.get("year_of_experience")
        education_required_text = "不拘"

        # Derive region and district from location_text
        region = None
        district = None
        if location_text:
            # Yourator locations are often just city names, e.g., "台北市"
            region = location_text
            district = location_text

        # Extract skills from description
        extracted_skills = []
        if description and SKILL_MASTER_DF is not None and not SKILL_MASTER_DF.empty:
            extracted_skills = extract_skills_precise(description, SKILL_MASTER_DF)

        return JobPydantic(
            source_platform=SourcePlatform.PLATFORM_YOURATOR,
            source_job_id=source_job_id,
            url=url,
            status=JobStatus.ACTIVE,
            title=title,
            description=description,
            job_type=job_type,
            posted_at=posted_at,
            salary_text=salary_text,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
            experience_required_text=experience_required_text,
            education_required_text=education_required_text,
            company=CompanyPydantic(
                source_platform=SourcePlatform.PLATFORM_YOURATOR,
                source_company_id=company_source_id,
                name=company_name,
                url=company_url,
            ),
            locations=[LocationPydantic(
                region=region,
                district=district,
                address_detail=location_text,
                latitude=None, # Yourator does not provide lat/lon
                longitude=None, # Yourator does not provide lat/lon
            )],
            skills=[SkillPydantic(name=skill_name) for skill_name in extracted_skills],
        )

    except Exception as e:
        logger.error(
            "Unexpected error when parsing Yourator job detail JSON.",
            event="unexpected_error_parsing_job_detail",
            error=str(e),
            job_data=job_data,
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="parser",
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
                    event="parse_date_format_error",
                    created_at=created_at_str,
                    job_id=source_job_id,
                    platform=SourcePlatform.PLATFORM_YOURATOR,
                    component="parser",
                )

        salary_text = f"{job_item.get('salary_min')} - {job_item.get('salary_max')} {job_item.get('salary_type')}"
        salary_min, salary_max, salary_type = parse_salary_text(salary_text)

        job_type_str = job_item.get("job_type")
        job_type = JOB_TYPE_MAPPING_YOURATOR.get(job_type_str, JobType.OTHER)

        experience_required_text = job_item.get("year_of_experience")
        education_required_text = "不拘"

        # Derive region and district from location_text
        region = None
        district = None
        if location_text:
            # Yourator locations are often just city names, e.g., "台北市"
            region = location_text
            district = location_text

        # Extract skills from description
        extracted_skills = []
        if description and SKILL_MASTER_DF is not None and not SKILL_MASTER_DF.empty:
            extracted_skills = extract_skills_precise(description, SKILL_MASTER_DF)

        return JobPydantic(
            source_platform=SourcePlatform.PLATFORM_YOURATOR,
            source_job_id=source_job_id,
            url=url,
            status=JobStatus.ACTIVE,
            title=title,
            description=description,
            job_type=job_type,
            posted_at=posted_at,
            salary_text=salary_text,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
            experience_required_text=experience_required_text,
            education_required_text=education_required_text,
            company=CompanyPydantic(
                source_platform=SourcePlatform.PLATFORM_YOURATOR,
                source_company_id=company_source_id,
                name=company_name,
                url=company_url,
            ),
            locations=[LocationPydantic(
                region=region,
                district=district,
                address_detail=location_text,
                latitude=None, # Yourator does not provide lat/lon
                longitude=None, # Yourator does not provide lat/lon
            )],
            skills=[SkillPydantic(name=skill_name) for skill_name in extracted_skills],
        )

    except Exception as e:
        logger.error(
            "Unexpected error when parsing Yourator job list JSON.",
            event="unexpected_error_parsing_job_list",
            error=str(e),
            job_item=job_item,
            platform=SourcePlatform.PLATFORM_YOURATOR,
            component="parser",
            exc_info=True,
        )
        return None
