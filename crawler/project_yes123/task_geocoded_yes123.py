import os

# python -m crawler.project_yes123.task_geocoded_yes123
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import structlog
import re
from typing import List
from crawler.database.connection import get_session
from crawler.database.models import Location
from crawler.database.repository import sync_job_observations_geocoding
from crawler.geocoding.client import geocode_address
from sqlalchemy import select

logger = structlog.get_logger(__name__)

def clean_address(address: str) -> str:
    """Cleans the address string by removing extraneous text."""
    if not address:
        return ""
    # 移除括號和括號內的所有內容
    address = re.sub(r"[\(（【].*?[\)）】]", "", address)
    # 移除「上班地點:」等前綴
    address = re.sub(r"^.*?上班地點:?", "", address)
    # 移除特殊符號和多餘的空格
    address = re.sub(r"[️]", "", address).strip()
    return address

def process_pending_geocoding_yes123(db_name: str = None, batch_size: int = 100):
    """
    從資料庫中獲取需要地理編碼的 Yes123 地點，進行地理編碼，並更新回資料庫。
    """
    logger.info("開始處理待地理編碼的 Yes123 地點。")
    total_updated_locations = 0

    while True:
        with get_session(db_name=db_name) as session:
            # 查詢所有 latitude 或 longitude 為 NULL 或空字串的地點
            locations_to_process: List[Location] = session.execute(
                select(Location).where(
                    (Location.latitude.is_(None)) | (Location.latitude == '') |
                    (Location.longitude.is_(None)) | (Location.longitude == '')
                ).limit(batch_size)
            ).scalars().all()

            if not locations_to_process:
                logger.info("沒有更多需要地理編碼的 Yes123 地點。")
                break

            logger.info(f"找到 {len(locations_to_process)} 個需要地理編碼的 Yes123 地點。")

            updated_locations_in_batch = 0
            for location in locations_to_process:
                if location.address_detail:
                    cleaned_address = clean_address(location.address_detail)
                    if not cleaned_address:
                        logger.warning("Address became empty after cleaning.", original_address=location.address_detail)
                        continue

                    coordinates = geocode_address(cleaned_address)
                    if coordinates:
                        location.latitude = str(coordinates["latitude"])
                        location.longitude = str(coordinates["longitude"])
                        updated_locations_in_batch += 1
                        logger.debug(
                            "地理編碼成功並標記更新。",
                            address=cleaned_address,
                            latitude=location.latitude,
                            longitude=location.longitude,
                        )
                    else:
                        logger.warning(
                            "地理編碼失敗。",
                            address=cleaned_address,
                        )
                else:
                    logger.warning(
                        "地點地址為空，無法進行地理編碼。",
                        location_id=location.id,
                    )
            
            if updated_locations_in_batch > 0:
                logger.info(f"準備提交 {updated_locations_in_batch} 個地點的地理編碼更新。")
                try:
                    session.commit()
                    logger.info(f"已成功提交 {updated_locations_in_batch} 個地點的地理編碼更新到資料庫。")
                    total_updated_locations += updated_locations_in_batch
                except Exception as e:
                    session.rollback()
                    logger.error(f"提交地理編碼更新時發生錯誤: {e}", exc_info=True)
                    # If commit fails, we might want to break or handle differently
                    # For now, we'll just log and continue to the next batch if possible
            else:
                logger.info("此批次沒有地點的地理編碼需要更新或提交。")
                # If no locations were updated in this batch, and there were locations to process,
                # it means geocoding failed for all of them. We can break to avoid infinite loops.
                if len(locations_to_process) > 0:
                    logger.warning("所有地點的地理編碼都失敗了，停止處理。")
                    break
    
    logger.info(f"地理編碼處理完成。總共更新了 {total_updated_locations} 個地點。")

def _run_local_test():
    db_name = os.environ.get('CRAWLER_DB_NAME', 'db_YES123')
    # 您可以在這裡設定 batch_size 的值，例如設定為 500
    # process_pending_geocoding_yes123(db_name=db_name, batch_size=500)
    process_pending_geocoding_yes123(db_name=db_name, batch_size=10)
    sync_job_observations_geocoding(db_name=db_name)

if __name__ == "__main__":
    _run_local_test()
