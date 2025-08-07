import pandas as pd
import json
import re
import os
import structlog
from multiprocessing import Pool, cpu_count

logger = structlog.get_logger(__name__)

def _process_job(job, compiled_skill_patterns):
    description_text = job.get('description', '')
    text_lower = description_text.lower()

    extracted_skills = []
    for compiled_regex, original_skill_name in compiled_skill_patterns:
        if compiled_regex.search(text_lower):
            extracted_skills.append(original_skill_name)
    
    # 確保技能唯一性並保留順序
    extracted_skills = list(dict.fromkeys(extracted_skills))
    
    processed_job = {
        "source_platform": job.get('source_platform', ''),
        "source_job_id": job.get('source_job_id', ''),
        "url": job.get('url', ''),
        "title": job.get('title', ''),
        "description": description_text,
        "skill": extracted_skills
    }
    return processed_job

def fetch_description_skill(input_json_path, output_json_path, skill_master_path):
    """
    從輸入的 JSON 檔案中擷取職缺描述的技能，並儲存到新的 JSON 檔案。

    Args:
        input_json_path (str): 輸入職缺 JSON 檔案的路徑 (例如: db_YES123.json)。
        output_json_path (str): 輸出職缺 JSON 檔案的路徑 (例如: db_YES123_skill.json)。
        skill_master_path (str): 技能主檔 skill_master.json 的路徑。
    """
    logger.info(f"開始從 {input_json_path} 擷取技能...")

    # 載入技能主檔
    if not os.path.exists(skill_master_path):
        logger.error(f"錯誤：找不到技能主檔。請先執行 `python3 -m skill_tool.run_skill_extraction --generate-kb` 來生成 {skill_master_path}")
        return
    
    try:
        skill_master_df = pd.read_json(skill_master_path)
        # 過濾掉過於通用的單字技能，例如 'c'
        skill_master_df = skill_master_df[skill_master_df['Skill_Name'].str.lower() != 'c']
        logger.info(f"已載入技能主檔: {skill_master_path}")
        logger.debug(f"skill_master_df shape: {skill_master_df.shape}")
    except Exception as e:
        logger.error(f"載入技能主檔失敗: {e}")
        return

    # 預處理技能列表，為每個技能預編譯正規表達式
    skill_master_df['Skill_Name_Lower'] = skill_master_df['Skill_Name'].str.lower()
    sorted_skills_for_matching = skill_master_df.sort_values(by='Skill_Name_Lower', key=lambda x: x.str.len(), ascending=False)

    compiled_skill_patterns = []
    skill_name_map = {}
    for _, row in sorted_skills_for_matching.iterrows():
        skill_name_lower = row['Skill_Name_Lower']
        original_skill_name = row['Skill_Name']
        skill_name_map[skill_name_lower] = original_skill_name

        is_chinese_skill = any('\u4e00' <= char <= '\u9fff' for char in original_skill_name)
        
        patterns_to_compile = []
        if is_chinese_skill:
            patterns_to_compile.append(re.escape(skill_name_lower))
        else:
            # 對於英文技能，同時考慮帶空格和不帶空格的匹配
            patterns_to_compile.append(r'\b' + re.escape(skill_name_lower) + r'\b')
            if ' ' in skill_name_lower:
                patterns_to_compile.append(r'\b' + re.escape(skill_name_lower.replace(' ', '')) + r'\b')
        
        for p in patterns_to_compile:
            compiled_skill_patterns.append((re.compile(p, re.IGNORECASE), original_skill_name))

    # 處理輸入職缺 JSON 檔案
    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            full_data = json.load(f)
            job_descriptions = []
            for item in full_data:
                if isinstance(item, dict) and item.get('type') == 'table' and item.get('name') == 'tb_jobs_YES123':
                    job_descriptions = item.get('data', [])
                    break
        if not job_descriptions:
            logger.error(f"錯誤：在檔案 {input_json_path} 中找不到 'tb_jobs_YES123' 表格的資料。")
            return
        logger.info(f"已載入職缺描述檔案: {input_json_path}")

    except Exception as e:
        logger.error(f"載入職缺描述檔案失敗: {e}")
        return

    # 使用 multiprocessing 平行處理職缺
    num_processes = cpu_count() # 獲取 CPU 核心數
    logger.info(f"使用 {num_processes} 個進程進行平行處理...")

    # 準備 _process_job 函式的參數
    # 每個元素是一個元組 (job, compiled_skill_patterns)
    job_args = [(job, compiled_skill_patterns) for job in job_descriptions]

    with Pool(num_processes) as pool:
        processed_jobs = pool.starmap(_process_job, job_args)

    # 儲存處理後的資料到新的 JSON 檔案
    try:
        os.makedirs(os.path.dirname(output_json_path), exist_ok=True)
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(processed_jobs, f, ensure_ascii=False, indent=2)
        logger.info(f"處理後的職缺資料已儲存至: {output_json_path}")
    except Exception as e:
        logger.error(f"儲存處理後的職缺資料失敗: {e}")
        return

if __name__ == "__main__":
    # 定義檔案路徑
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(current_dir, '..') # 假設專案根目錄在 crawler 的上一層

    input_file = os.path.join(project_root, 'db_YES123.json')
    output_file = os.path.join(project_root, 'tb_jobs_yes123_skill.json') # 輸出到專案根目錄
    skill_master_file = os.path.join(project_root, 'skill_tool', 'generated_data', 'skill_master.json')

    fetch_description_skill(input_file, output_file, skill_master_file)
