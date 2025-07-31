import re
from datetime import datetime
from typing import Optional, Tuple
import structlog
from bs4 import BeautifulSoup

from crawler.database.schemas import (
    JobPydantic,
    SourcePlatform,
    JobStatus,
    JobType,
    SalaryType,
)
from crawler.project_1111.config_1111 import JOB_DETAIL_BASE_URL_1111

logger = structlog.get_logger(__name__)

# 1111 API 的 jobType 到我們內部 JobType Enum 的映射
JOB_TYPE_MAPPING_1111 = {
    "全職": JobType.FULL_TIME,
    "兼職": JobType.PART_TIME,
    "實習": JobType.INTERNSHIP,
    "派遣": JobType.CONTRACT,
    "約聘": JobType.TEMPORARY,
    "其他": JobType.OTHER,
}

# 1111 API 的 jobType (整數) 到字串的映射
JOB_TYPE_INT_TO_STR_MAPPING_1111 = {
    1: "全職",
    2: "兼職",
    3: "實習",
    4: "派遣",
    5: "約聘",
}

# 1111 API 的教育程度 (grades) 映射
EDUCATION_MAPPING_1111 = {
    2: "高中",
    8: "專科",
    16: "大學",
    32: "碩士",
    64: "博士",
}


def parse_salary(
    salary_text: str,
) -> Tuple[Optional[int], Optional[int], Optional[SalaryType]]:
    salary_min, salary_max, salary_type = None, None, None
    text = salary_text.replace(",", "").replace(" ", "").lower()

    # Handle "面議（經常性薪資達4萬元或以上"
    match_negotiable_with_min = re.search(r"面議\(經常性薪資達(\d+)元或以上\)", text)
    if match_negotiable_with_min:
        salary_min = int(match_negotiable_with_min.group(1))
        salary_type = SalaryType.MONTHLY # Assuming monthly for this case
        return salary_min, None, salary_type

    # Monthly Salary
    match_monthly = re.search(r"月薪(\d+)(?:元)?(?:[至~])?(\d+)?元?", text)
    if match_monthly:
        salary_type = SalaryType.MONTHLY
        salary_min = int(match_monthly.group(1))
        if match_monthly.group(2):
            salary_max = int(match_monthly.group(2))
        elif "以上" in text:
            salary_max = None # No upper bound
        else:
            salary_max = salary_min # Single value
        return salary_min, salary_max, salary_type

    # Hourly Salary
    match_hourly = re.search(r"時薪(\d+)(?:元)?(?:[至~])?(\d+)?元?", text)
    if match_hourly:
        salary_type = SalaryType.HOURLY
        salary_min = int(match_hourly.group(1))
        if match_hourly.group(2):
            salary_max = int(match_hourly.group(2))
        else:
            salary_max = salary_min
        return salary_min, salary_max, salary_type

    # Yearly Salary
    match_yearly = re.search(r"年薪(\d+)萬(?:[至~])?(\d+)?萬?", text)
    if match_yearly:
        salary_type = SalaryType.YEARLY
        salary_min = int(match_yearly.group(1)) * 10000
        if match_yearly.group(2):
            salary_max = int(match_yearly.group(2)) * 10000
        else:
            salary_max = None
        return salary_min, salary_max, salary_type

    # Daily Salary
    match_daily = re.search(r"日薪(\d+)元", text)
    if match_daily:
        salary_type = SalaryType.DAILY
        salary_min = int(match_daily.group(1))
        salary_max = int(match_daily.group(1))
        return salary_min, salary_max, salary_type

    # Other types
    if "論件計酬" in text:
        salary_type = SalaryType.BY_CASE
        return None, None, salary_type
    if "面議" in text:
        salary_type = SalaryType.NEGOTIABLE
        return None, None, SalaryType.NEGOTIABLE

    # Fallback logic for salary type
    if salary_min is not None and salary_type is None:
        if salary_min < 20000:
            salary_type = SalaryType.HOURLY
        elif salary_min > 200000:
            salary_type = SalaryType.YEARLY
        else:
            salary_type = SalaryType.MONTHLY

    return salary_min, salary_max, salary_type


def parse_job_list_json_to_pydantic(job_item: dict) -> Optional[JobPydantic]:
    """
    從 1111 列表頁 API 的 JSON 數據解析並轉換為 JobPydantic 物件。
    """
    try:
        job_id = str(job_item.get("jobId"))
        company_source_id = str(job_item.get("companyId"))

        url = f"{JOB_DETAIL_BASE_URL_1111}{job_id}"
        company_url = f"https://www.1111.com.tw/corp/{company_source_id}"

        title = job_item.get("title")
        company_name = job_item.get("companyName")
        description = job_item.get("description")
        location_text = job_item.get("workCity", {}).get("name", "").strip() or None

        posted_at = None
        update_at_str = job_item.get("updateAt")
        if update_at_str:
            try:
                posted_at = datetime.strptime(update_at_str, "%Y/%m/%d %H:%M:%S")
            except ValueError:
                logger.warning(
                    "Could not parse posted_at date format from list API.",
                    update_at=update_at_str,
                    job_id=job_id,
                )

        salary_text = job_item.get("salary", "")
        salary_min, salary_max, salary_type = parse_salary(salary_text)

        job_type_int = job_item.get("jobType")
        job_type_str = JOB_TYPE_INT_TO_STR_MAPPING_1111.get(job_type_int)
        job_type = JOB_TYPE_MAPPING_1111.get(job_type_str) if job_type_str else None

        experience_required_text = job_item.get("require", {}).get("experience")
        if experience_required_text == "0":
            experience_required_text = "不拘"
        elif experience_required_text is None:
            experience_required_text = "不拘"

        education_required_text = "不拘"
        education_grades = job_item.get("require", {}).get("grades")
        if education_grades and isinstance(education_grades, list):
            valid_grades = sorted(
                [g for g in education_grades if g in EDUCATION_MAPPING_1111]
            )
            if valid_grades:
                min_edu_code = min(valid_grades)
                education_required_text = EDUCATION_MAPPING_1111.get(
                    min_edu_code, "不拘"
                )
                if any(g > min_edu_code for g in valid_grades):
                    education_required_text += "以上"

        job_pydantic_data = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_1111,
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
            experience_required_text=str(experience_required_text),
            education_required_text=education_required_text,
            company_source_id=company_source_id,
            company_name=company_name,
            company_url=company_url,
        )
        return job_pydantic_data

    except Exception as e:
        logger.error(
            "Unexpected error when parsing 1111 job list JSON.",
            error=e,
            job_item=job_item,
            exc_info=True,
        )
        return None


def parse_job_detail_html_to_pydantic(
    html_content: str, url: str
) -> Optional[JobPydantic]:
    """
    從 1111 職缺頁面的 HTML 內容解析並轉換為 JobPydantic 物件。
    """
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        job_id = url.split("/")[-1].split("?")[0]

        # --- 1. 頁首區塊 (Top Section) ---
        header_section = soup.select_one(
            "section[data-v-e57f1019] > div.container > div.text-gray-600"
        )

        title = None
        company_name = None
        job_type_str = None
        salary_text = None
        education_required_text = None
        location_text = None
        posted_at = None
        experience_required_text = None
        description = None
        company_url = None
        company_source_id = None

        if header_section:
            title = header_section.select_one("h1").get_text(strip=True) or None
            company_name = (
                header_section.select_one("h2.inline").get_text(strip=True) or None
            )
            # Extract company_url from company_name's parent a tag
            company_link_tag = header_section.select_one("h2.inline a")
            if company_link_tag and "href" in company_link_tag.attrs:
                company_url = company_link_tag["href"]
                # Extract company_source_id from company_url if possible
                match = re.search(r"/corp/(\d+)", company_url)
                if match:
                    company_source_id = match.group(1)

            pills = header_section.select("div.flex.flex-wrap.mt-4.gap-3 > div")
            top_info = [p.get_text(strip=True, separator=" ") for p in pills]
            if len(top_info) >= 4:
                job_type_str = top_info[0]  # e.g., "全職"
                salary_text = top_info[1]
                education_required_text = top_info[2]
                location_text = top_info[3]

            info_items = header_section.select("ul.info-item > li")
            for item in info_items:
                key_tag = item.select_one("h3")
                val_tag = item.select_one("span") or item.select_one("time")
                if key_tag and val_tag:
                    key = key_tag.get_text(strip=True)
                    value = val_tag.get_text(strip=True)
                    if "更新日期" in key:
                        try:
                            posted_at = datetime.strptime(
                                value.replace(" ", ""), "%Y/%m/%d"
                            )
                        except ValueError:
                            logger.warning(
                                "Could not parse posted_at date format.",
                                value=value,
                                job_id=job_id,
                            )
                    elif "工作經驗" in key:
                        experience_required_text = value

        # --- 2. 主要內容區塊 (Main Content Sections) ---
        sections = soup.select("section[id]")
        for section in sections:
            section_title_tag = section.select_one("h2.text-lg.text-main")
            if not section_title_tag:
                continue

            section_title = section_title_tag.get_text(strip=True)

            if section_title == "工作內容":
                # Find the job description within the '工作內容' section
                job_description_h3 = section.find(
                    "h3", string=lambda t: t and "職缺描述" in t
                )
                if job_description_h3:
                    description_container = job_description_h3.find_next_sibling()
                    if description_container:
                        description = description_container.get_text(
                            separator="\n", strip=True
                        )

        salary_min, salary_max, salary_type = parse_salary(salary_text or "")
        job_type = JOB_TYPE_MAPPING_1111.get(job_type_str)
        if job_type is None:
            job_type = JobType.OTHER

        if experience_required_text is None:
            experience_required_text = "不拘"
        if education_required_text is None:
            education_required_text = "不拘"

        job_pydantic_data = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_1111,
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
            company_source_id=company_source_id,
            company_name=company_name,
            company_url=company_url,
        )
        return job_pydantic_data

    except Exception as e:
        logger.error(
            "Unexpected error when parsing 1111 job HTML.",
            error=e,
            url=url,
            exc_info=True,
        )
        return None
