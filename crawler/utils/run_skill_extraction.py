import pandas as pd
import json
import re
import os
import structlog
import argparse

logger = structlog.get_logger(__name__)

AMBIGUOUS_SKILLS = ["r"]

# ... (AMBIGUOUS_SKILLS_CONTEXT remains the same) ...

def generate_knowledge_base_for_powerbi(topic_tree_path, csv_path, category_source_path, major_categories_path, output_dir):
    """
    Generates two files for Power BI: a master skill table and a skill-to-job mapping table.
    """
    logger.info("開始生成 Power BI 知識庫...")

    # --- 1. Load all data sources ---
    logger.info("  - 正在讀取所有資料來源...")
    df_skill_cat = pd.read_csv(csv_path)
    with open(category_source_path, 'r', encoding='utf-8') as f:
        category_data = json.load(f)
        actual_data = next((item['data'] for item in category_data if isinstance(item, dict) and item.get('type') == 'table'), None)
        if actual_data:
            df_cat_source = pd.DataFrame(actual_data)
        else:
            raise ValueError("Could not find the data table in category_source.json")
    df_major_cat = pd.read_json(major_categories_path)
    with open(topic_tree_path, 'r', encoding='utf-8') as f:
        topic_data = json.load(f)

    # --- 2. Process 104 Data to create a Skill-Job mapping ---
    logger.info("  - 正在處理 104 資料並建立技能-職務對應...")
    df_skill_cat = df_skill_cat.dropna(subset=['jobCode', 'hardSkillList'])
    df_skill_cat['jobCode'] = df_skill_cat['jobCode'].astype(str)
    df_cat_source['source_category_id'] = df_cat_source['source_category_id'].astype(str)

    # Create mapping tables
    l2_to_l1_map = df_cat_source.set_index('source_category_id')['parent_source_id'].to_dict()
    l1_id_to_name_map = df_major_cat.set_index('source_category_id')['source_category_name'].to_dict()
    l2_id_to_name_map = df_cat_source.set_index('source_category_id')['source_category_name'].to_dict()

    all_skills_data = []
    platform_104_skill_job_map = {} # To store skill to 104 job category IDs

    for _, row in df_skill_cat.iterrows():
        try:
            skills = json.loads(row['hardSkillList'].replace("'", '"'))
            job_code = row['jobCode']
            job_name = row['jobName']
            
            l2_id = l2_to_l1_map.get(job_code)
            l1_id = l2_to_l1_map.get(l2_id)
            l1_name = l1_id_to_name_map.get(l1_id, "")
            l2_name = l2_id_to_name_map.get(l2_id, "")

            for skill in skills:
                skill_name = skill.get('name').strip().lower()
                if skill_name:
                    all_skills_data.append({
                        "Skill_Name": skill_name,
                        "L1_Category": l1_name,
                        "L2_Category": l2_name,
                        "L3_Category": job_name,
                        "Source": "PLATFORM_104"
                    })
                    if skill_name not in platform_104_skill_job_map:
                        platform_104_skill_job_map[skill_name] = []
                    platform_104_skill_job_map[skill_name].append(job_code)
        except (json.JSONDecodeError, KeyError, AttributeError):
            continue

    # --- 3. Process Topic Tree Data ---
    logger.info("  - 正在處理 topic_tree 資料...")
    def parse_topic_tree_recursively(data, path, flat_list):
        if isinstance(data, dict):
            for key, value in data.items():
                parse_topic_tree_recursively(value, path + [key], flat_list)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    match = re.match(r'([^(]+)(?:\((.*)\))?', item)
                    if match:
                        main_skill = match.group(1).strip().lower()
                        flat_list.append({
                            "Skill_Name": main_skill,
                            "L1_Category": path[0] if len(path) > 0 else "",
                            "L2_Category": path[1] if len(path) > 1 else "",
                            "L3_Category": path[2] if len(path) > 2 else "",
                            "Source": "topic_tree"
                        })

    topic_tree_skills = []
    parse_topic_tree_recursively(topic_data, [], topic_tree_skills)
    all_skills_data.extend(topic_tree_skills)

    # --- 4. Create and Save Master Skill Table ---
    logger.info("  - 正在建立並儲存技能主檔...")
    df_master = pd.DataFrame(all_skills_data)
    # Prioritize topic_tree source
    df_master = df_master.sort_values('Source', ascending=False).drop_duplicates(subset=['Skill_Name'], keep='first')
    output_master_file = os.path.join(output_dir, "skill_master.json")
    df_master.to_json(output_master_file, orient='records', force_ascii=False, indent=2)
    logger.info(f"    -> 技能主檔已儲存至: {output_master_file}")

    # --- 5. Create and Save Skill-to-Job Mapping Table ---
    logger.info("  - 正在建立並儲存技能-職務關聯表...")
    
    # Get all unique skills from the master skill table
    all_unique_skills = df_master['Skill_Name'].unique()

    skill_to_job_mapping_list = []
    for skill_name in all_unique_skills:
        mapping_entry = {
            "Skill_Name": skill_name,
            "PLATFORM_104": platform_104_skill_job_map.get(skill_name, []),
            "PLATFORM_1111": [], # Placeholder for future data
            "PLATFORM_YES123": [], # Placeholder for future data
            "PLATFORM_CAKERESUME": [] # Placeholder for future data
        }
        skill_to_job_mapping_list.append(mapping_entry)

    df_mapping = pd.DataFrame(skill_to_job_mapping_list)
    output_mapping_file = os.path.join(output_dir, "skill_to_job_mapping.json")
    df_mapping.to_json(output_mapping_file, orient='records', force_ascii=False, indent=2)
    logger.info(f"    -> 技能-職務關聯表已儲存至: {output_mapping_file}")

# Placeholder for AMBIGUOUS_SKILLS_CONTEXT if it's truly missing or complex
AMBIGUOUS_SKILLS_CONTEXT = {} # Or load from a file if it exists

def create_enhanced_knowledge_base(topic_tree_path, csv_path, category_source_path, major_categories_path, output_dir):
    # This function is a placeholder if it was part of the original script but not provided.
    # The generate_knowledge_base_for_powerbi already handles the main KB generation.
    logger.info("Creating enhanced knowledge base (placeholder)...")
    pass

def preprocess_skills_for_extraction(skill_master_df):
    """
    Preprocesses the skill master DataFrame to create pre-compiled regex patterns for efficient skill extraction.
    Returns a list of (compiled_regex, original_skill_name) tuples.
    """
    if skill_master_df.empty:
        return []

    # Ensure 'Skill_Name' column exists
    if 'Skill_Name' not in skill_master_df.columns:
        logger.error("skill_master_df must contain a 'Skill_Name' column.")
        return []

    skill_master_df['Skill_Name_Lower'] = skill_master_df['Skill_Name'].str.lower()
    # Sort skills by length in descending order to prioritize longer, more specific matches
    sorted_skills_for_matching = skill_master_df.sort_values(
        by='Skill_Name_Lower', key=lambda x: x.str.len(), ascending=False
    )

    compiled_skill_patterns = []
    for _, row in sorted_skills_for_matching.iterrows():
        skill_name_lower = row['Skill_Name_Lower']
        original_skill_name = row['Skill_Name']

        # Check if the skill name contains Chinese characters (non-ASCII)
        is_chinese_skill = any('\u4e00' <= char <= '\u9fff' for char in original_skill_name)

        if is_chinese_skill:
            # For Chinese skills, use a simple substring match after escaping special regex characters
            pattern = re.escape(skill_name_lower)
        elif skill_name_lower in AMBIGUOUS_SKILLS:
            # For ambiguous English skills, use a more precise boundary check
            pattern = r'(?<![a-zA-Z0-9])' + re.escape(skill_name_lower) + r'(?![a-zA-Z0-9])'
        else:
            # For other English skills, use word boundaries
            pattern = r'\b' + re.escape(skill_name_lower) + r'\b'
        
        compiled_skill_patterns.append((re.compile(pattern, re.IGNORECASE), original_skill_name))
    
    return compiled_skill_patterns

def get_compiled_skill_patterns():
    """
    Loads the skill master JSON and preprocesses it to return compiled regex patterns.
    """
    skill_master_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'skill_data',
        'generated_data',
        'skill_master.json'
    )

    if not os.path.exists(skill_master_path):
        logger.error(f"錯誤：找不到技能主檔。請先執行 `python3 -m skill_tool.run_skill_extraction --generate-kb` 來生成 {skill_master_path}")
        return []
    
    try:
        skill_master_df = pd.read_json(skill_master_path)
        logger.info(f"已載入技能主檔: {skill_master_path}")
        return preprocess_skills_for_extraction(skill_master_df)
    except Exception as e:
        logger.error(f"載入技能主檔或編譯技能模式失敗: {e}")
        return []

def extract_skills_precise(text, compiled_skill_patterns):
    """
    Extracts skills from a given text using pre-compiled regex patterns.
    """
    extracted_skills = []
    if not isinstance(text, str) or not compiled_skill_patterns:
        return extracted_skills

    text_lower = text.lower()

    for compiled_regex, original_skill_name in compiled_skill_patterns:
        if compiled_regex.search(text_lower):
            extracted_skills.append(original_skill_name)
            # Optional: Remove the matched skill from the text to avoid re-matching substrings
            # text_lower = compiled_regex.sub("", text_lower) # Use sub on the compiled regex

    return list(dict.fromkeys(extracted_skills)) # Return unique skills while preserving order (Python 3.7+)




# ... (create_enhanced_knowledge_base and extract_skills_precise remain the same for now) ...

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract skills from job descriptions and manage the skill knowledge base.")
    parser.add_argument(
        "input_json", 
        nargs='?', # Make input_json optional
        type=str, 
        help="Path to the input job descriptions JSON file."
    )
    parser.add_argument(
        "--topic_tree", 
        type=str, 
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'skill_data', 'source_data', 'topic_tree.json'),
        help="Path to the topic tree JSON file."
    )
    parser.add_argument(
        "--csv_knowledge", 
        type=str, 
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'skill_data', 'source_data', '104_skill_category.csv'),
        help="Path to the 104 skill category CSV file."
    )
    parser.add_argument(
        "--category_source", 
        type=str, 
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'skill_data', 'source_data', 'tb_category_source.json'),
        help="Path to the category source JSON file."
    )
    parser.add_argument(
        "--major_categories", 
        type=str, 
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'skill_data', 'source_data', 'major_categories.json'),
        help="Path to the major categories JSON file."
    )
    parser.add_argument(
        "--generate-kb",
        action='store_true',
        help="If set, the script will only generate the master skill knowledge base for Power BI and exit."
    )

    args = parser.parse_args()

    if args.generate_kb:
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'skill_data', 'generated_data')
        os.makedirs(output_dir, exist_ok=True) # Ensure the output directory exists
        generate_knowledge_base_for_powerbi(
            args.topic_tree, 
            args.csv_knowledge, 
            args.category_source, 
            args.major_categories, 
            output_dir
        )
        exit()

    if not args.input_json:
        logger.error("錯誤：未提供輸入的職缺 JSON 檔案。請提供檔案路徑或使用 --generate-kb 選項。")
        exit()

    # Define output directory for generated files
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'skill_data', 'generated_data')
    os.makedirs(output_dir, exist_ok=True)

    # Load skill_master.json for skill extraction
    skill_master_path = os.path.join(output_dir, "skill_master.json")
    if not os.path.exists(skill_master_path):
        logger.error(f"錯誤：找不到技能主檔。請先執行 `python3 -m skill_tool.run_skill_extraction --generate-kb` 來生成 {skill_master_path}")
        exit()
    
    try:
        skill_master_df = pd.read_json(skill_master_path)
        logger.info(f"已載入技能主檔: {skill_master_path}")
        logger.debug(f"skill_master_df shape: {skill_master_df.shape}")
        logger.debug(f"skill_master_df head:\n{skill_master_df.head()}")
    except Exception as e:
        logger.error(f"載入技能主檔失敗: {e}")
        exit()

    # Process input job descriptions JSON file
    try:
        with open(args.input_json, 'r', encoding='utf-8') as f:
            full_data = json.load(f)
            job_descriptions = []
            for item in full_data:
                if isinstance(item, dict) and item.get('type') == 'table' and item.get('name') == 'tb_jobs_cakeresume':
                    job_descriptions = item.get('data', [])
                    break
        if not job_descriptions:
            logger.error(f"錯誤：在檔案 {args.input_json} 中找不到 'tb_jobs_cakeresume' 表格的資料。")
            exit()
        logger.info(f"已載入職缺描述檔案: {args.input_json}")

        # --- Temporary debug: Print first few job descriptions ---
        # --- End of temporary debug ---

    except Exception as e:
        logger.error(f"載入職缺描述檔案失敗: {e}")
        exit()

    # Pre-process skill_master_df once for efficient extraction
    skill_master_df['Skill_Name_Lower'] = skill_master_df['Skill_Name'].str.lower()
    # Sort skills by length in descending order to prioritize longer, more specific matches
    # This sorting is still useful for ensuring that longer, more specific skills are found first
    # if there are overlapping skills (e.g., "Python" and "Python Django").
    sorted_skills_for_matching = skill_master_df.sort_values(by='Skill_Name_Lower', key=lambda x: x.str.len(), ascending=False)

    # Pre-compile individual regex patterns for each skill
    compiled_skill_patterns = []
    skill_name_map = {} # To map lowercased skill name back to original case
    for _, row in sorted_skills_for_matching.iterrows():
        skill_name_lower = row['Skill_Name_Lower']
        original_skill_name = row['Skill_Name']
        skill_name_map[skill_name_lower] = original_skill_name

        is_chinese_skill = any('\u4e00' <= char <= '\u9fff' for char in original_skill_name)
        if is_chinese_skill:
            pattern = re.escape(skill_name_lower)
        else:
            pattern = r'\b' + re.escape(skill_name_lower) + r'\b'
        compiled_skill_patterns.append((re.compile(pattern, re.IGNORECASE), original_skill_name))

    processed_jobs = []
    for i, job in enumerate(job_descriptions):
        if i % 100 == 0: # Log every 100 jobs
            logger.info(f"Processing job {i+1}/{len(job_descriptions)}...")

        description_text = job.get('description', '')
        text_lower = description_text.lower() # Convert description to lower once per job

        extracted_skills = []
        # Iterate through pre-compiled patterns and check for each skill
        for compiled_regex, original_skill_name in compiled_skill_patterns:
            if compiled_regex.search(text_lower):
                extracted_skills.append(original_skill_name)
        
        # Ensure unique skills (order is naturally preserved by the loop)
        extracted_skills = list(dict.fromkeys(extracted_skills))

        
        # Create a new dictionary with desired fields and extracted skills
        processed_job = {
            "source_platform": job.get('source_platform', ''),
            "url": job.get('url', ''),
            "source_job_id": job.get('source_job_id', ''),
            "title": job.get('title', ''),
            "description": description_text,
            "skill": extracted_skills
        }
        processed_jobs.append(processed_job)

    # Save the processed data to a new JSON file
    input_filename = os.path.basename(args.input_json)
    output_filename = os.path.splitext(input_filename)[0] + "_with_skills.json"
    output_file_path = os.path.join(output_dir, output_filename)

    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(processed_jobs, f, ensure_ascii=False, indent=2)
        logger.info(f"處理後的職缺資料已儲存至: {output_file_path}")
    except Exception as e:
        logger.error(f"儲存處理後的職缺資料失敗: {e}")
        exit()
