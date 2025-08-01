# crawler/project_cakeresume/parser_cakeresume.py
"""
Parsers for Cakeresume, handling data transformation from the __NEXT_DATA__ script tag.
"""
import structlog
from datetime import datetime
from typing import Optional, Dict, Any
from urllib.parse import urljoin
import re
from bs4 import BeautifulSoup

from crawler.database.schemas import JobPydantic, SourcePlatform, JobStatus, SalaryType, JobType
from crawler.utils.clean_text import clean_text

logger = structlog.get_logger(__name__)

def _parse_cakeresume_salary(
    job_details: Dict[str, Any]
) -> tuple[Optional[int], Optional[int], Optional[SalaryType], Optional[str]]:
    """Parses salary information from Cakeresume's structured data."""
    salary_min_raw = job_details.get("salary_min")
    salary_max_raw = job_details.get("salary_max")
    salary_type_raw = job_details.get("salary_type")
    salary_currency = job_details.get("salary_currency")

    salary_min = int(salary_min_raw) if isinstance(salary_min_raw, (int, float)) else None
    salary_max = int(salary_max_raw) if isinstance(salary_max_raw, (int, float)) else None

    salary_type_map = {
        'per_month': SalaryType.MONTHLY,
        'per_year': SalaryType.YEARLY,
        'per_hour': SalaryType.HOURLY,
        'per_day': SalaryType.DAILY,
        'piece_rate_pay': SalaryType.BY_CASE,
    }
    salary_type = salary_type_map.get(str(salary_type_raw)) if salary_type_raw else None

    salary_text = None
    if salary_min is not None and salary_max is not None and salary_type and salary_currency:
        if salary_min == 0 and salary_max == 0:
            salary_text = "面議"
            salary_type = SalaryType.NEGOTIABLE
        elif salary_min == salary_max:
            salary_text = f"{salary_currency} {salary_min:,}"
        else:
            salary_text = f"{salary_currency} {salary_min:,} ~ {salary_max:,}"
        
        if salary_type != SalaryType.NEGOTIABLE:
            type_text_map = {
                SalaryType.MONTHLY: " / 月",
                SalaryType.YEARLY: " / 年",
                SalaryType.HOURLY: " / 時",
                SalaryType.DAILY: " / 日",
            }
            salary_text += type_text_map.get(salary_type, "")
    elif job_details.get("hide_salary_completely") or "面議" in str(job_details):
        salary_text = "面議"
        salary_type = SalaryType.NEGOTIABLE

    return salary_min, salary_max, salary_type, salary_text

def _parse_job_type(job_details: Dict[str, Any]) -> JobType:
    """Maps job type text to JobType enum."""
    job_type_raw = job_details.get("job_type")
    job_type_map = {
        "full_time": JobType.FULL_TIME,
        "part_time": JobType.PART_TIME,
        "contract": JobType.CONTRACT,
        "internship": JobType.INTERNSHIP,
        "temporary": JobType.TEMPORARY,
        "freelance": JobType.CONTRACT,
    }
    return job_type_map.get(str(job_type_raw), JobType.OTHER)

def parse_job_details_to_pydantic(job_details: Dict[str, Any], html_content: str, url: str) -> Optional[JobPydantic]:
    """
    Parses the job data extracted from the __NEXT_DATA__ script tag into a JobPydantic object.
    """
    try:
        source_job_id = str(job_details.get("path"))
        if not source_job_id:
            match = re.search(r'jobs/([a-zA-Z0-9_-]+)', url)
            if match:
                source_job_id = match.group(1)
            else:
                logger.warning("Could not determine source_job_id from JSON or URL.", url=url)
                return None

        company_data = job_details.get("company", {})
        company_name = company_data.get("name")
        company_path = company_data.get("path")
        company_url = f"https://www.cakeresume.com/companies/{company_path}" if company_path else None

        description_html = job_details.get("description", "")
        description_plain = job_details.get("description_plain_text", "")
        description = clean_text(description_html) or clean_text(description_plain)

        soup = BeautifulSoup(html_content, "html.parser")

        if not company_name:
            company_tag = soup.select_one(".JobDescriptionLeftColumn_name__ABAp9")
            if company_tag:
                company_name = company_tag.get_text(strip=True)
        
        if not company_url:
            company_link = soup.select_one("a.JobDescriptionLeftColumn_name__ABAp9")
            if company_link and company_link.has_attr('href'):
                company_url = urljoin("https://www.cakeresume.com", company_link['href'])
        
        if not company_path:
            if company_url:
                company_path = company_url.split("/companies/")[-1]

        description_html = job_details.get("description", "")
        location_tags = soup.select("div.JobDescriptionRightColumn_locationsWrapper__N_fz_ a")
        location_text = ", ".join([clean_text(tag.get_text()) for tag in location_tags]) if location_tags else None

        posted_at_raw = job_details.get("content_updated_at")
        posted_at = None
        if posted_at_raw:
            try:
                posted_at = datetime.fromisoformat(str(posted_at_raw).replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                logger.warning("Failed to parse 'content_updated_at' date.", value=posted_at_raw)

        salary_min, salary_max, salary_type, salary_text = _parse_cakeresume_salary(job_details)

        if not salary_text:
            salary_tag = soup.select_one(".JobDescriptionRightColumn_salaryWrapper__Q_8IL span")
            if salary_tag:
                salary_text = salary_tag.get_text(strip=True)
                # Attempt to parse min and max from the scraped text
                numbers = [int(s) for s in re.findall(r'\d+', salary_text.replace(",", ""))]
                if len(numbers) == 2:
                    salary_min, salary_max = numbers
                elif len(numbers) == 1:
                    salary_min = numbers[0]
                if "月" in salary_text:
                    salary_type = SalaryType.MONTHLY

        min_exp_year = job_details.get("min_work_exp_year")
        experience_required_text = f"{int(min_exp_year)} 年以上" if isinstance(min_exp_year, int) and min_exp_year > 0 else "不拘"

        requirements_text = job_details.get("requirements_plain_text", "")
        edu_match = re.search(r'(高中|專科|大學|碩士|博士)', requirements_text)
        education_required_text = edu_match.group(1) if edu_match else "不拘"

        return JobPydantic(
            source_platform=SourcePlatform.PLATFORM_CAKERESUME,
            source_job_id=source_job_id,
            url=url,
            status=JobStatus.ACTIVE,
            title=clean_text(job_details.get("title")),
            description=description,
            job_type=_parse_job_type(job_details),
            location_text=location_text,
            posted_at=posted_at,
            salary_text=salary_text,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
            experience_required_text=experience_required_text,
            education_required_text=education_required_text,
            company_source_id=company_path,
            company_name=clean_text(company_name),
            company_url=company_url,
        )

    except Exception as e:
        logger.error("Failed to parse Cakeresume script JSON.", url=url, error=e, exc_info=True)
        return None
