import re
from datetime import datetime
from typing import Optional
import structlog
import pandas as pd
import os
from crawler.database.scripts.clean_address_detail import clean_address

from crawler.database.schemas import (
    JobPydantic,
    CompanyPydantic,
    LocationPydantic,
    SourcePlatform,
    JobStatus,
    JobType,
    SalaryType,
    SkillPydantic,
)
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


# 104 API 的 jobType 到我們內部 JobType Enum 的映射
JOB_TYPE_MAPPING = {
    0: JobType.FULL_TIME, # 0 也代表全職
    1: JobType.FULL_TIME,
    2: JobType.PART_TIME,
    3: JobType.INTERNSHIP,
    4: JobType.CONTRACT,  # 派遣
    5: JobType.TEMPORARY,  # 兼職/計時
}

# 104 API 的教育程度 (optionEdu) 映射
EDUCATION_MAPPING_104 = {
    1: "不拘",
    2: "國中",
    3: "高中",
    4: "專科",
    5: "大學",
    6: "碩士",
    7: "博士",
}

# 104 API 的工作經驗 (period) 映射
EXPERIENCE_MAPPING_104 = {
    0: "不拘",
    1: "1年以下",
    2: "1-3年",
    3: "3-5年",
    4: "5-10年",
    5: "10年以上",
}


def parse_salary(
    salary_text: str,
) -> (Optional[int], Optional[int], Optional[SalaryType]):
    salary_min, salary_max, salary_type = None, None, None
    text = salary_text.replace(",", "").lower()

    # 月薪 (範圍)
    match_monthly_range = re.search(r"月薪([0-9]+)(?:[至~])([0-9]+)元", text)
    if match_monthly_range:
        salary_type = SalaryType.MONTHLY
        salary_min = int(match_monthly_range.group(1))
        salary_max = int(match_monthly_range.group(2))
        return salary_min, salary_max, salary_type

    # 月薪 (單一數值)
    match_monthly_single = re.search(r"月薪([0-9]+)元", text)
    if match_monthly_single:
        salary_type = SalaryType.MONTHLY
        salary_min = int(match_monthly_single.group(1))
        salary_max = int(match_monthly_single.group(1))
        return salary_min, salary_max, salary_type

    # 月薪 (以上)
    match_monthly_above = re.search(r"月薪([0-9]+)元以上", text)
    if match_monthly_above:
        salary_type = SalaryType.MONTHLY
        salary_min = int(match_monthly_above.group(1))
        salary_max = 9999999 # 設定一個足夠大的上限值
        return salary_min, salary_max, salary_type

    # 年薪
    match_yearly = re.search(r"年薪([0-9]+)萬(?:[至~])([0-9]+)萬", text) or re.search(
        r"年薪([0-9]+)萬以上", text
    )
    if match_yearly:
        salary_type = SalaryType.YEARLY
        salary_min = int(match_yearly.group(1)) * 10000
        if len(match_yearly.groups()) > 1 and match_yearly.group(2):
            salary_max = int(match_yearly.group(2)) * 10000
        return salary_min, salary_max, salary_type

    # 時薪
    match_hourly = re.search(r"時薪([0-9]+)元", text)
    if match_hourly:
        salary_type = SalaryType.HOURLY
        salary_min = int(match_hourly.group(1))
        salary_max = int(match_hourly.group(1))
        return salary_min, salary_max, salary_type

    # 日薪
    match_daily = re.search(r"日薪([0-9]+)元", text)
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


def parse_job_item_to_pydantic(job_item: dict) -> Optional[JobPydantic]:
    """
    從 104 API 的單一職缺項目(dict)解析並轉換為 JobPydantic 物件。
    此函式可處理來自「列表頁 API」和「單一職缺 API」的回應。
    """
    try:
        is_single_job_api = "header" in job_item and "jobDetail" in job_item

        if is_single_job_api:
            header = job_item.get("header", {})
            job_detail = job_item.get("jobDetail", {})
            condition = job_item.get("condition", {})
            company_data = job_item.get("company", {})

            url = job_item.get("link", {}).get("job") or header.get("analysisUrl")
            job_id = url.split('/')[-1] if url else None # Extract from URL

            cust_url = header.get("custUrl", "")
            source_company_id = cust_url.split('/')[-1].split('?')[0] if cust_url else None

            cust_name = header.get("custName")
            title = header.get("jobName")
            description = job_detail.get("jobDescription")
            raw_job_type = job_detail.get("jobType")
            
            address_region_text = job_detail.get("addressRegion", "")
            address_detail_text = job_detail.get("addressDetail", "")
            full_address = clean_address(address_region_text + address_detail_text)

            region = address_region_text[:3] if address_region_text else None
            district = address_region_text if address_region_text else None

            latitude = job_detail.get("latitude")
            longitude = job_detail.get("longitude")

            appear_date_str = header.get("appearDate")
            salary_text_raw = job_detail.get("salary", "")
            experience_required_text = condition.get("workExp")
            education_required_text = condition.get("edu")
            industry = company_data.get("industryDesc")

        else:  # 來自列表頁 API
            url = job_item.get("link", {}).get("job", "")
            job_id = url.split('/')[-1] if url else None # Extract from URL

            cust_url = job_item.get("link", {}).get("cust", "")
            source_company_id = cust_url.split('/')[-1].split('?')[0] if cust_url else None
            
            cust_name = job_item.get("custName")
            title = job_item.get("jobName")
            description = job_item.get("description")
            raw_job_type = job_item.get("jobType")

            address_region_text = job_item.get("jobAddrNoDesc", "")
            address_detail_text = job_item.get("jobAddress", "")
            full_address = clean_address(address_region_text + address_detail_text)

            region = address_region_text[:3] if address_region_text else None
            district = address_region_text if address_region_text else None

            latitude = job_item.get("lat")
            longitude = job_item.get("lon")

            appear_date_str = job_item.get("appearDate")
            industry = job_item.get("coIndustryDesc")

            salary_text_raw = ""
            salary_low = job_item.get("salaryLow")
            salary_high = job_item.get("salaryHigh")

            if salary_low is not None and salary_high is not None:
                if salary_low == 0 and salary_high == 0:
                    salary_text_raw = "面議"
                elif salary_low == salary_high:
                    salary_text_raw = f"月薪{salary_low}元"
                else:
                    salary_text_raw = f"月薪{salary_low}至{salary_high}元"
            elif salary_low is not None:
                salary_text_raw = f"月薪{salary_low}元以上"

            raw_education = job_item.get("optionEdu")
            if raw_education and isinstance(raw_education, list):
                if 1 in raw_education:
                    education_required_text = "不拘"
                else:
                    min_edu_code = min(raw_education)
                    education_required_text = EDUCATION_MAPPING_104.get(min_edu_code, "不拘")
                    if any(code > min_edu_code for code in raw_education):
                        education_required_text += "以上"
            else:
                education_required_text = "不拘"

            raw_experience = job_item.get("period")
            experience_required_text = EXPERIENCE_MAPPING_104.get(raw_experience, "不拘")

        if not job_id:
            logger.warning("Missing job_id in job_item or URL.", job_item_keys=job_item.keys(), url=url)
            return None

        posted_at = None
        if appear_date_str:
            try:
                posted_at = datetime.strptime(appear_date_str, "%Y/%m/%d")
            except ValueError:
                try:
                    posted_at = datetime.strptime(appear_date_str, "%Y%m%d")
                except ValueError:
                    logger.warning("Could not parse posted_at date format.", appear_date=appear_date_str, job_id=job_id)

        salary_min, salary_max, salary_type = parse_salary(salary_text_raw)
        job_type = JOB_TYPE_MAPPING.get(raw_job_type, JobType.OTHER)

        # Ensure source_company_id is a string
        source_company_id_str = str(source_company_id) if source_company_id is not None else None

        company_pydantic = CompanyPydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_company_id=source_company_id_str,
            name=cust_name,
            url=cust_url,
            industry=industry,
        )

        location_pydantic = LocationPydantic(
            region=region,
            district=district,
            address_detail=full_address,
            latitude=str(latitude) if latitude else None,
            longitude=str(longitude) if longitude else None,
        )

        # Extract skills from description
        extracted_skills = []
        if description and SKILL_MASTER_DF is not None and not SKILL_MASTER_DF.empty:
            extracted_skills = extract_skills_precise(description, SKILL_MASTER_DF)

        job_pydantic = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_job_id=str(job_id), # Ensure source_job_id is a string
            url=url,
            status=JobStatus.ACTIVE,
            title=title,
            description=description,
            job_type=job_type,
            posted_at=posted_at,
            salary_text=salary_text_raw,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
            experience_required_text=experience_required_text,
            education_required_text=education_required_text,
            company=company_pydantic,
            locations=[location_pydantic],
            skills=[SkillPydantic(name=skill_name) for skill_name in extracted_skills],
            category_tags=[str(cat_id) for cat_id in job_item.get("jobCat", [])] # Convert to string
        )

        return job_pydantic

    except (AttributeError, KeyError) as e:
        logger.error("Missing key fields when parsing job_item.", error=e, job_id=job_item.get("jobNo", "N/A"), exc_info=True)
        return None
    except Exception as e:
        logger.error("Unexpected error when parsing job_item.", error=e, job_id=job_item.get("jobNo", "N/A"), exc_info=True)
        return None
