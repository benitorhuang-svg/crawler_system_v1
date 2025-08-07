import json
import os
import structlog

logger = structlog.get_logger(__name__)

def remove_fields_from_json(json_path, fields_to_remove):
    """
    從 JSON 檔案中移除指定的欄位。
    """
    logger.info(f"開始從 {json_path} 移除指定欄位...")
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 遍歷每個物件並移除指定欄位
        for item in data:
            for field in fields_to_remove:
                if field in item:
                    del item[field]
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"成功從 {json_path} 移除欄位: {fields_to_remove}")
    except Exception as e:
        logger.error(f"移除欄位失敗: {e}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file = os.path.join(current_dir, 'generated_data', '104_skill_category.json')
    fields = ["jobPic", "jobWorkerId", "jobWorkerIdList", "isCollection"]
    remove_fields_from_json(json_file, fields)
