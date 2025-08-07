import pandas as pd
import os
import structlog

logger = structlog.get_logger(__name__)

def convert_csv_to_json(csv_path, json_path):
    """
    將 CSV 檔案轉換為 JSON 格式，並儲存到指定路徑。
    """
    logger.info(f"開始將 {csv_path} 轉換為 JSON 格式...")
    try:
        df = pd.read_csv(csv_path)
        # 將 DataFrame 轉換為 JSON 格式
        # orient='records' 會將每一行轉換為一個 JSON 物件
        # force_ascii=False 允許非 ASCII 字元 (如中文)
        # indent=2 為了可讀性進行縮排
        df.to_json(json_path, orient='records', force_ascii=False, indent=2)
        logger.info(f"成功將 CSV 轉換為 JSON，儲存至: {json_path}")
    except Exception as e:
        logger.error(f"轉換 CSV 到 JSON 失敗: {e}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(current_dir, 'source_data', '104_skill_category.csv')
    json_output_dir = os.path.join(current_dir, 'generated_data')
    json_output_file = os.path.join(json_output_dir, '104_skill_category.json')

    os.makedirs(json_output_dir, exist_ok=True)
    convert_csv_to_json(csv_file, json_output_file)
