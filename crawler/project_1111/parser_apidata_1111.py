import os

#  python -m crawler.project_1111.task_urls_1111
# --- Local Test Environment Setup ---
if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
# --- End Local Test Environment Setup ---


import re
from datetime import datetime, timezone
from typing import Optional, Tuple

import structlog
import pandas as pd
from bs4 import BeautifulSoup

# 假設這些 Pydantic 模型和設定檔都存在於您的專案結構中
from crawler.database.schemas import (
    JobPydantic,
    SourcePlatform,
    JobStatus,
    JobType,
    LocationPydantic,
    SkillPydantic,
    CompanyPydantic,
)
from crawler.project_1111.config_1111 import JOB_DETAIL_BASE_URL_1111
from crawler.utils.salary_parser import parse_salary_text
from crawler.utils.run_skill_extraction import extract_skills_precise, preprocess_skills_for_extraction

# 日誌設定
logger = structlog.get_logger(__name__)

# --- 常數定義 (Constants) ---

# 技能主檔路徑
SKILL_MASTER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'utils',
    'skill_data',
    'generated_data',
    'skill_master.json'
)

# 1111 API 的資料映射
JOB_TYPE_MAPPING_1111 = {
    "全職": JobType.FULL_TIME, "兼職": JobType.PART_TIME, "實習": JobType.INTERNSHIP,
    "派遣": JobType.CONTRACT, "約聘": JobType.TEMPORARY, "其他": JobType.OTHER,
}
JOB_TYPE_INT_TO_STR_MAPPING_1111 = {1: "全職", 2: "兼職", 3: "實習", 4: "派遣", 5: "約聘"}
EDUCATION_MAPPING_1111 = {2: "高中", 8: "專科", 16: "大學", 32: "碩士", 64: "博士"}

# HTML 詳情頁的 CSS 選擇器
class DetailPageSelectors:
    HEADER_SECTION = "section[data-v-e57f1019] > div.container > div.text-gray-600"
    TITLE = "h1"
    COMPANY_NAME_LINK = "h2.inline a"
    LOCATION_MAP_IFRAME = "iframe"
    DESCRIPTION_CONTAINER = "h3:-soup-contains('職缺描述') ~ div"

# --- 輔助函式 (Helper Functions) ---

def _load_skill_master_data() -> pd.DataFrame:
    """
    載入技能主檔資料。如果檔案不存在或發生錯誤，則記錄錯誤並返回一個空的 DataFrame。
    """
    if not os.path.exists(SKILL_MASTER_PATH):
        logger.error(
            "技能主檔遺失，將跳過技能提取。請執行 `python3 -m skill_tool.run_skill_extraction --generate-kb` 來生成檔案。",
            path=SKILL_MASTER_PATH
        )
        return pd.DataFrame()
    try:
        df = pd.read_json(SKILL_MASTER_PATH)
        logger.info(f"已成功載入技能主檔: {SKILL_MASTER_PATH}")
        return df
    except Exception as e:
        logger.error("載入技能主檔時發生未預期錯誤", error=e, path=SKILL_MASTER_PATH)
        return pd.DataFrame()

def _parse_full_address(full_address: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    從完整地址字串中解析出「縣市」和「鄉鎮市區」。
    """
    if not full_address:
        return None, None

    # 正則表達式優先匹配 '縣市' 和 '鄉鎮市區'
    match = re.match(r'(?P<region>.+?[縣市])(?P<district>.+?[鄉鎮市區])', full_address)
    if match:
        return match.group('region'), match.group('district')

    # 若正則不匹配，則進行簡易分割
    if '市' in full_address:
        parts = full_address.split('市', 1)
        return f"{parts[0]}市", f"{parts[0]}市" # 若無法細分，則區也設為市
    if '縣' in full_address:
        parts = full_address.split('縣', 1)
        return f"{parts[0]}縣", f"{parts[0]}縣"

    return full_address, full_address # 最後的備案

# --- 全域變數初始化 ---
SKILL_MASTER_DF = _load_skill_master_data()
COMPILED_SKILL_PATTERNS = preprocess_skills_for_extraction(SKILL_MASTER_DF)


# --- 主要解析函式 (Main Parsers) ---

def parse_job_list_json_to_pydantic(job_item: dict) -> Optional[JobPydantic]:
    """
    從 1111 職缺列表 API 的 JSON 資料解析並轉換為 JobPydantic 物件。
    """
    try:
        job_id = str(job_item.get("jobId"))
        company_source_id = str(job_item.get("companyId"))

        url = f"{JOB_DETAIL_BASE_URL_1111}{job_id}"
        company_url = f"https://www.1111.com.tw/corp/{company_source_id}"
        title = job_item.get("title")
        company_name = job_item.get("companyName")
        description = job_item.get("description")

        posted_at = None
        if update_at_str := job_item.get("updateAt"):
            try:
                local_dt = datetime.strptime(update_at_str, "%Y/%m/%d %H:%M:%S")
                posted_at = local_dt.astimezone(timezone.utc)
            except (ValueError, TypeError):
                logger.warning("無法解析列表 API 的 posted_at 日期格式", update_at=update_at_str, job_id=job_id)

        salary_text = job_item.get("salary", "")
        salary_min, salary_max, salary_type = parse_salary_text(salary_text)

        job_type_int = job_item.get("jobType")
        job_type_str = JOB_TYPE_INT_TO_STR_MAPPING_1111.get(job_type_int)
        job_type = JOB_TYPE_MAPPING_1111.get(job_type_str, JobType.OTHER)

        experience_text = job_item.get("require", {}).get("experience")
        experience_required_text = "不拘" if experience_text in [None, "0"] else experience_text

        education_required_text = "不拘"
        if education_grades := job_item.get("require", {}).get("grades", []):
            valid_grades = sorted([g for g in education_grades if g in EDUCATION_MAPPING_1111])
            if valid_grades:
                min_edu_code = min(valid_grades)
                education_required_text = EDUCATION_MAPPING_1111.get(min_edu_code, "不拘")
                if any(g > min_edu_code for g in valid_grades):
                    education_required_text += "以上"

        location_text = job_item.get("workCity", {}).get("name", "").strip() or None
        region, district = _parse_full_address(location_text)
        locations = [LocationPydantic(region=region, district=district, address_detail=location_text)]

        skills = []
        if description and COMPILED_SKILL_PATTERNS:
            extracted_skills = extract_skills_precise(description, COMPILED_SKILL_PATTERNS)
            skills = [SkillPydantic(name=skill_name) for skill_name in extracted_skills]

        return JobPydantic(
            source_platform=SourcePlatform.PLATFORM_1111,
            source_job_id=job_id,
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
            experience_required_text=str(experience_required_text),
            education_required_text=education_required_text,
            company=CompanyPydantic(
                source_platform=SourcePlatform.PLATFORM_1111,
                source_company_id=company_source_id,
                name=company_name,
                url=company_url,
                industry_id=job_item.get("industry", {}).get("id"),
            ),
            locations=locations,
            skills=skills,
        )

    except Exception as e:
        logger.error("解析 1111 職缺列表 JSON 時發生錯誤", error=e, job_item=job_item, exc_info=True)
        return None


def parse_job_detail_html_to_pydantic(
    html_content: str,
    url: str,
    existing_job: Optional[JobPydantic] = None
) -> Optional[JobPydantic]:
    """
    從 1111 職缺詳情頁的 HTML 內容解析並更新 JobPydantic 物件。
    此函式會優先使用詳情頁的資料來覆蓋或填充列表頁可能缺失的資訊。
    """
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        job_id = url.split("/")[-1].split("?")[0]

        job_data = existing_job or JobPydantic(
            source_platform=SourcePlatform.PLATFORM_1111,
            source_job_id=job_id,
            url=url,
            status=JobStatus.ACTIVE,
            title="",
        )

        # --- 1. 頁首區塊 (Header Section) ---
        if header := soup.select_one(DetailPageSelectors.HEADER_SECTION):
            if not job_data.title and (title_tag := header.select_one(DetailPageSelectors.TITLE)):
                job_data.title = title_tag.get_text(strip=True)
            
            if company_link := header.select_one(DetailPageSelectors.COMPANY_NAME_LINK):
                company_name = company_link.get_text(strip=True)
                company_url = company_link.get("href")
                company_source_id = None
                if company_url and (match := re.search(r"/corp/(\d+)", company_url)):
                    company_source_id = match.group(1)

                if not job_data.company:
                    job_data.company = CompanyPydantic(source_platform=SourcePlatform.PLATFORM_1111)
                
                job_data.company.name = job_data.company.name or company_name
                job_data.company.url = job_data.company.url or company_url
                job_data.company.source_company_id = job_data.company.source_company_id or company_source_id

        # --- 2. 工作條件區塊 (Job Condition Section) ---
        full_address, latitude, longitude = None, None, None
        
        # 尋找 "工作地點" 這個標題來定位
        location_title_tag = soup.find("h3", string=lambda text: text and "工作地點" in text.strip())
        if location_title_tag and (content_container := location_title_tag.find_next_sibling("div")):
            
            # 【核心修正】處理地址內包含多餘空格和換行的情況
            if address_tag := content_container.find("p"):
                # 使用 split() 和 join() 來移除所有內嵌的空白字元
                full_address = "".join(address_tag.get_text().split())
            
            if iframe := content_container.select_one(DetailPageSelectors.LOCATION_MAP_IFRAME):
                if src := iframe.get("src"):
                    if match := re.search(r'q=([0-9.-]+),([0-9.-]+)', src):
                        latitude, longitude = match.group(1), match.group(2)
        
        # 更新地點資訊 (只有在從 HTML 成功獲取到新資訊時才更新)
        if full_address or latitude or longitude:
            region, district = _parse_full_address(full_address)
            if not job_data.locations:
                job_data.locations = [LocationPydantic()]
            
            loc = job_data.locations[0]
            loc.address_detail = full_address or loc.address_detail
            loc.latitude = latitude or loc.latitude
            loc.longitude = longitude or loc.longitude
            # 只有在地址更新後，才重新解析縣市區域
            if full_address:
                loc.region, loc.district = region, district

        # 更新發布時間 (如果列表頁沒有提供)
        if not job_data.posted_at:
            if posted_at_tag := soup.find(string=re.compile(r"職缺更新")):
                posted_at_text = posted_at_tag.strip().replace("職缺更新：", "")
                try:
                    if "今天" in posted_at_text:
                        job_data.posted_at = datetime.now(timezone.utc)
                    else:
                        current_year = datetime.now(timezone.utc).year
                        # 處理 YYYY.MM.DD 或 MM.DD 格式
                        date_str = posted_at_text if '.' in posted_at_text[4:] else f"{current_year}.{posted_at_text}"
                        parsed_date = datetime.strptime(date_str, "%Y.%m.%d")
                        job_data.posted_at = parsed_date.replace(tzinfo=timezone.utc)
                except ValueError:
                    logger.warning("無法解析詳情頁的 posted_at 日期格式", value=posted_at_text, job_id=job_id)

        # --- 3. 工作內容與技能 (Description and Skills) ---
        # 使用詳情頁更完整的描述來覆蓋列表頁的簡述
        description_section_title = soup.find("h2", string=lambda t: t and "工作內容" in t.strip())
        if description_section_title:
            if desc_container := description_section_title.find_next_sibling("div"):
                 if desc_content_container := desc_container.select_one(DetailPageSelectors.DESCRIPTION_CONTAINER):
                    job_data.description = desc_content_container.get_text(separator="\n", strip=True)

        # 基於更新後的描述，重新提取技能
        if job_data.description and COMPILED_SKILL_PATTERNS:
            extracted_skills = extract_skills_precise(job_data.description, COMPILED_SKILL_PATTERNS)
            job_data.skills = [SkillPydantic(name=skill_name) for skill_name in extracted_skills]

        return job_data

    except Exception as e:
        logger.error("解析 1111 職缺詳情頁 HTML 時發生錯誤", error=e, url=url, exc_info=True)
        return None