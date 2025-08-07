import json
import os
import structlog
import ast # Used for safely evaluating string literals containing Python structures

logger = structlog.get_logger(__name__)

def process_skill_lists(json_path, fields_to_process):
    """
    從 JSON 檔案中處理指定的技能列表欄位，只保留 'name' 的值。
    """
    logger.info(f"開始處理 {json_path} 中的技能列表欄位...")
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for item in data:
            for field in fields_to_process:
                if field in item and isinstance(item[field], str):
                    try:
                        # Safely evaluate the string literal into a Python list
                        list_data = ast.literal_eval(item[field])
                        
                        # Extract only the 'name' value if it's a list of dictionaries
                        if isinstance(list_data, list):
                            processed_names = []
                            for entry in list_data:
                                if isinstance(entry, dict) and 'name' in entry:
                                    processed_names.append(entry['name'])
                                elif isinstance(entry, str): # Handle cases where it might just be a list of strings
                                    processed_names.append(entry)
                            item[field] = processed_names
                        else:
                            # If it's not a list (e.g., just an empty string or other unexpected format), keep it as is or set to empty list
                            item[field] = []
                    except (ValueError, SyntaxError) as e:
                        logger.warning(f"處理欄位 {field} 時發生錯誤: {item[field]} - {e}. 將其設置為空列表。")
                        item[field] = []
                elif field in item and item[field] is None: # Handle null values
                    item[field] = []

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"成功處理 {json_path} 中的技能列表欄位。")
    except Exception as e:
        logger.error(f"處理技能列表欄位失敗: {e}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file = os.path.join(current_dir, 'generated_data', '104_skill_category.json')
    fields = ["hardToolList", "hardSkillList", "hardCertList"]
    process_skill_lists(json_file, fields)
