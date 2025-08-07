import json
import sys
import os
import argparse
from datetime import datetime, timezone
from typing import Optional

# 為了能夠導入 crawler 模組，需要將專案根目錄添加到 Python 路徑
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from crawler.geocoding.client import geocode_address
from crawler.database.connection import initialize_database
from crawler.database.schemas import (
    SourcePlatform,
    JobPydantic,
    JobStatus,
    JobType,
    LocationPydantic,
    CompanyPydantic,
    SkillPydantic,
)
from crawler.database.repository import (
    upsert_jobs,
)
from crawler.utils.salary_parser import parse_salary_text
from crawler.utils.run_skill_extraction import extract_skills_precise, preprocess_skills_for_extraction
import pandas as pd
import structlog

# 配置日誌
structlog.configure(
    processors=[
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger(__name__)

# --- Skill master data loading ---
SKILL_MASTER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'crawler', # Assuming this script is in the root, and skill_data is under crawler/utils
    'utils',
    'skill_data',
    'generated_data',
    'skill_master.json'
)
SKILL_MASTER_DF = pd.DataFrame()
COMPILED_SKILL_PATTERNS = []

try:
    SKILL_MASTER_DF = pd.read_json(SKILL_MASTER_PATH)
    logger.info(f"已載入技能主檔: {SKILL_MASTER_PATH}")
    COMPILED_SKILL_PATTERNS = preprocess_skills_for_extraction(SKILL_MASTER_DF)
    logger.info("已預處理技能模式。")
except FileNotFoundError:
    logger.warning(f"警告：找不到技能主檔。請先執行 `python3 -m skill_tool.run_skill_extraction --generate-kb` 來生成 {SKILL_MASTER_PATH}")
except Exception as e:
    logger.error(f"載入或預處理技能主檔失敗: {e}")


def map_yes123_job_data_to_pydantic(job_data: dict) -> Optional[JobPydantic]:
    """
    將從 yes123 JSON 讀取的職位資料映射到 JobPydantic 物件。
    """
    try:
        # 假設 source_platform 可以從 job_data 中獲取，或者預設為 PLATFORM_YES123
        source_platform_name = job_data.get("source_platform", "PLATFORM_YES123")
        source_platform = SourcePlatform[source_platform_name]

        source_job_id = job_data.get("source_job_id") or job_data.get("id")
        if not source_job_id:
            logger.warning("Skipping job due to missing source_job_id or id.", job_data=job_data)
            return None

        url = job_data.get("url")
        title = job_data.get("title")
        description = job_data.get("description")

        # 薪資解析
        salary_text = job_data.get("salary_text", "")
        salary_min, salary_max, salary_type = parse_salary_text(salary_text)

        # 日期解析
        posted_at_str = job_data.get("posted_at")
        posted_at = None
        if posted_at_str:
            try:
                # 嘗試解析多種日期格式
                posted_at = datetime.strptime(posted_at_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    posted_at = datetime.fromisoformat(posted_at_str).replace(tzinfo=timezone.utc)
                except ValueError:
                    logger.warning(f"無法解析 posted_at 日期格式: {posted_at_str}")

        # 公司資訊
        company_name = job_data.get("company_name")
        company_url = job_data.get("company_url")
        company_source_id = job_data.get("company_source_id") or company_name # 使用 company_name 作為 fallback

        company_pydantic = None
        if company_name:
            company_pydantic = CompanyPydantic(
                source_platform=source_platform,
                source_company_id=company_source_id,
                name=company_name,
                url=company_url,
            )

        # 地點資訊 (從 location_text 獲取經緯度)
        location_text = job_data.get("location_text")
        latitude = None
        longitude = None
        region = None
        district = None

        if location_text:
            if len(location_text) >= 3:
                region = location_text[:3]
            district = location_text # 暫時將整個 location_text 作為 district

            coordinates = geocode_address(location_text)
            if coordinates:
                latitude = str(coordinates["latitude"])
                longitude = str(coordinates["longitude"])
            else:
                logger.warning(f"無法獲取 {location_text} 的經緯度。")

        location_pydantic = LocationPydantic(
            region=region,
            district=district,
            address_detail=location_text,
            latitude=latitude,
            longitude=longitude,
        )

        # 技能提取
        extracted_skills = []
        if description and COMPILED_SKILL_PATTERNS: # Use COMPILED_SKILL_PATTERNS here
            extracted_skills = extract_skills_precise(description, COMPILED_SKILL_PATTERNS)
        skills_pydantic = [SkillPydantic(name=skill_name) for skill_name in extracted_skills]

        # 職務類別 (假設 category_tags 存在於 job_data 中，或者需要從其他地方獲取)
        # 這裡假設 job_data 中有一個 'category_tags' 欄位，如果沒有，則為空列表
        category_tags = job_data.get("category_tags", [])
        # 如果 job_data 中沒有 category_tags，但有 source_category_id，則使用它
        if not category_tags and job_data.get("source_category_id"):
            category_tags = [job_data.get("source_category_id")]

        return JobPydantic(
            source_platform=source_platform,
            source_job_id=source_job_id,
            url=url,
            title=title,
            description=description,
            job_type=JobType.FULL_TIME, # 這裡可能需要更精確的映射
            posted_at=posted_at,
            status=JobStatus.ACTIVE, # 預設為 ACTIVE
            salary_text=salary_text,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
            experience_required_text=job_data.get("experience_required_text"),
            education_required_text=job_data.get("education_required_text"),
            company=company_pydantic,
            locations=[location_pydantic],
            skills=skills_pydantic,
            category_tags=category_tags,
        )
    except Exception as e:
        logger.error(f"映射職位資料到 Pydantic 失敗: {e}", job_data=job_data, exc_info=True)
        return None


def process_json_file_to_db(input_filepath: str):
    logger.info(f"開始處理檔案並寫入資料庫: {input_filepath}")
    
    try:
        with open(input_filepath, 'r', encoding='utf-8') as f:
            full_json_data = json.load(f)
        
        logger.debug(f"載入的 JSON 資料類型: {type(full_json_data)}")
        if isinstance(full_json_data, list) and full_json_data:
            logger.debug(f"載入的 JSON 資料第一個元素類型: {type(full_json_data[0])}")
            if isinstance(full_json_data[0], dict):
                logger.debug(f"載入的 JSON 資料第一個元素鍵: {full_json_data[0].keys()}")

        jobs_data_raw = []
        # 判斷 JSON 結構是直接的職位陣列還是包含 "table" 物件的陣列
        if isinstance(full_json_data, list) and full_json_data and isinstance(full_json_data[0], dict) and ("source_job_id" in full_json_data[0] or "id" in full_json_data[0]):
            jobs_data_raw = full_json_data
            logger.info(f"偵測到直接的職位資料陣列，共 {len(jobs_data_raw)} 筆記錄。")
        else:
            # 尋找 type 為 "table" 的物件，並提取其 "data" 陣列
            for item in full_json_data:
                if item.get("type") == "table":
                    # 假設表格名稱是 tb_jobs_yes123 或 tb_jobs_cakeresume
                    table_name = item.get("name")
                    if table_name and ("tb_jobs_yes123" in table_name.lower() or "tb_jobs_cakeresume" in table_name.lower()):
                        jobs_data_raw = item.get("data", [])
                        logger.info(f"找到表格 '{table_name}' 的資料，共 {len(jobs_data_raw)} 筆記錄。")
                        break
        
        if not jobs_data_raw:
            logger.error("在輸入 JSON 檔案中找不到任何職位表格的資料。")
            return

        total_jobs = len(jobs_data_raw)
        logger.info(f"總共有 {total_jobs} 筆職位記錄需要處理。")

        # 初始化資料庫連接
        initialize_database()

        jobs_to_upsert = []
        for i, job_raw in enumerate(jobs_data_raw):
            logger.info(f"[{i+1}/{total_jobs}] 正在處理職位: {job_raw.get('title', 'N/A')} (ID: {job_raw.get('id', 'N/A')})")
            
            job_pydantic = map_yes123_job_data_to_pydantic(job_raw)
            
            if job_pydantic:
                jobs_to_upsert.append(job_pydantic)
            else:
                logger.warning(f"跳過職位記錄，因為無法映射到 Pydantic: {job_raw.get('id', 'N/A')}")

        if jobs_to_upsert:
            logger.info(f"準備 upsert {len(jobs_to_upsert)} 筆職位資料到資料庫。")
            upsert_jobs(jobs_to_upsert)
            logger.info("所有職位資料及其關聯已成功 upsert 到資料庫。")
        else:
            logger.info("沒有職位資料需要 upsert。")

    except FileNotFoundError:
        logger.error(f"錯誤: 找不到檔案 {input_filepath}")
    except json.JSONDecodeError as e:
        logger.error(f"錯誤: 無法解析 JSON 檔案 {input_filepath} - {e}")
    except Exception as e:
        logger.error(f"處理過程中發生未知錯誤: {e}", exc_info=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="處理 phpMyAdmin 匯出的 JSON 檔案，進行地理編碼並寫入資料庫。")
    parser.add_argument("input_file", help="輸入的 JSON 檔案路徑 (例如: tb_jobs_yes123.json)")
    
    args = parser.parse_args()
    
    process_json_file_to_db(args.input_file)