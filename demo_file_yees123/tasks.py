# crawler/projects/platform_yes123/tasks.py
"""Yes123平台的分類抓取器 (Category Fetcher)。

此任務通過請求 Yes123 提供的 `work_mode.json` API 端點來獲取
結構化的職務分類數據，並將其扁平化後同步到資料庫。
"""
import logging
from typing import List, Dict, Any

from crawler.app import app
from crawler.enums import SourcePlatform
from crawler.database import repository
from crawler.utils import make_request
from crawler.settings import settings

logger = logging.getLogger(__name__)

def parse_and_flatten_categories(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    根據 yes123 `work_mode.json` 的實際結構，解析並扁平化分類數據。
    
    Args:
        raw_data (Dict[str, Any]): 從 API 獲取的原始 JSON 數據。

    Returns:
        List[Dict[str, Any]]: 一個扁平化的、符合資料庫模型的字典列表。
    """
    flat_list = []
    
    # 數據包裹在 'listObj' 鍵中
    level_1_list = raw_data.get("listObj")
    if not isinstance(level_1_list, list):
        logger.warning("[Yes123] JSON 數據中缺少 'listObj' 鍵或其不是一個列表。")
        return []

    for level_1_item in level_1_list:
        level_1_name = level_1_item.get("level_1_name")
        if not level_1_name:
            continue

        # 為一級分類創建一個記錄。我們使用其名稱作為代理 ID。
        level_1_proxy_id = level_1_name
        flat_list.append({
            "source_platform": SourcePlatform.PLATFORM_YES123,
            "source_category_id": level_1_proxy_id,
            "source_category_name": level_1_name,
            "parent_source_id": None,
        })
        
        level_2_list = level_1_item.get("list_2")
        if not isinstance(level_2_list, list):
            continue

        for level_2_item in level_2_list:
            level_2_code = level_2_item.get("code")
            level_2_name = level_2_item.get("level_2_name")
            
            if level_2_code and level_2_name:
                # 為二級分類創建記錄，並將其 parent_source_id 指向一級分類的代理 ID
                flat_list.append({
                    "source_platform": SourcePlatform.PLATFORM_YES123,
                    "source_category_id": level_2_code,
                    "source_category_name": level_2_name,
                    "parent_source_id": level_1_proxy_id,
                })
                
    return flat_list


@app.task(bind=True, name="platform_yes123.run_category_pipeline", acks_late=True, time_limit=600)
def run_category_pipeline(self) -> None:
    """從 Yes123 API 獲取職務分類並同步到資料庫。"""
    try:
        logger.info("[Yes123] 開始執行分類抓取 pipeline (from API)...")
        cfg = settings.pyes123
        api_url = "https://www.yes123.com.tw/json_file/work_mode.json"
        
        res = make_request(api_url, headers=cfg.headers)
        res.encoding = 'utf-8-sig' # 保持對 BOM 的處理
        json_data = res.json()
        
        # [修正] 使用新的解析函數
        transformed_data = parse_and_flatten_categories(json_data)
        
        if not transformed_data:
            logger.warning("[Yes123] 未能從 API 中提取任何分類數據。")
            return

        result = repository.sync_source_categories(SourcePlatform.PLATFORM_YES123, transformed_data)
        logger.info(f"[Yes123] 分類 pipeline 成功完成。同步結果: {result}")

    except Exception as e:
        logger.error(f"[Yes123] 分類 pipeline 失敗: {e}", exc_info=True)
        raise self.retry(exc=e, countdown=180)