import os
import structlog
from typing import List
from crawler.database.connection import get_session
from crawler.database.models import Location
from crawler.geocoding.client import geocode_address
from sqlalchemy import select

logger = structlog.get_logger(__name__)

def process_pending_geocoding(db_name: str = None, batch_size: int = 100):
    """
    從資料庫中獲取需要地理編碼的地點，進行地理編碼，並更新回資料庫。
    """
    logger.info("開始處理待地理編碼的地點。")
    
    with get_session(db_name=db_name) as session:
        # 查詢所有 latitude 或 longitude 為 NULL 的地點
        locations_to_process: List[Location] = session.execute(
            select(Location).where(
                (Location.latitude.is_(None)) | (Location.longitude.is_(None))
            ).limit(batch_size)
        ).scalars().all()

        if not locations_to_process:
            logger.info("沒有需要地理編碼的地點。")
            return

        logger.info(f"找到 {len(locations_to_process)} 個需要地理編碼的地點。")

        for location in locations_to_process:
            if location.address_detail:
                coordinates = geocode_address(location.address_detail)
                if coordinates:
                    location.latitude = str(coordinates["latitude"])
                    location.longitude = str(coordinates["longitude"])
                    logger.debug(
                        "地理編碼成功。",
                        address=location.address_detail,
                        latitude=location.latitude,
                        longitude=location.longitude,
                    )
                else:
                    logger.warning(
                        "地理編碼失敗。",
                        address=location.address_detail,
                    )
            else:
                logger.warning(
                    "地點地址為空，無法進行地理編碼。",
                    location_id=location.id,
                )
        
        session.commit()
        logger.info(f"已處理 {len(locations_to_process)} 個地點的地理編碼。")

if __name__ == "__main__":
    # 這裡可以根據需要設定資料庫名稱，例如從環境變數獲取
    db_name_for_local_run = os.environ.get('CRAWLER_DB_NAME', 'test_db')
    process_pending_geocoding(db_name=db_name_for_local_run)
