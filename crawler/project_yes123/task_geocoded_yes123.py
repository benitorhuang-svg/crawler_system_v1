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
from crawler.database.models import Location, Job, JobLocation, Url
from crawler.database.repository import sync_job_observations_geocoding
from crawler.geocoding.client import geocode_address, load_geocoding_cache, save_geocoding_cache
from sqlalchemy import select, func
import asyncio
from functools import partial
from crawler.database.schemas import CrawlStatus
from datetime import datetime, timedelta, timezone
from crawler.config import GEOCODING_RETRY_FAILED_DURATION_HOURS
from sqlalchemy.orm import aliased # Import aliased

logger = structlog.get_logger(__name__)

def clean_address(address: str) -> str:
    """Cleans the address string by removing extraneous text and irrelevant details."""
    if not address:
        return ""

    original_address = address

    # 移除括號和括號內的所有內容
    address = re.sub(r"[\(（【].*?[\)）】]", "", address)
    # 移除樓層資訊 (例如: 78樓, 11樓)
    address = re.sub(r"\d+樓", "", address)
    # 移除常見的非地址描述性文字和特殊符號
    address = re.sub(r"依不同地區自行前往|或在總公司搭乘公司車輛前往作業地點|北中南都有工作據點|須能隨案場做變動|能外宿者|工作地點將依公司安排進行分派，並非最終確定地點，未來有可能會變更|→", "", address)
    # 移除特殊符號和多餘的空格
    address = re.sub(r"[️]", "", address).strip()

    # 如果清理後地址變得很短或看起來不像地址，則可能需要進一步判斷或保留原始地址的一部分
    # 這裡可以根據實際情況調整，例如設定最小長度或檢查是否包含關鍵字
    if len(address) < 5 and len(original_address) > 5:
        logger.warning("Address became too short after cleaning, might indicate over-cleaning.", original_address=original_address, cleaned_address=address)

    return address


async def process_pending_geocoding_yes123(db_name: str = None, batch_size: int = 100):
    """
    從資料庫中獲取需要地理編碼的 Yes123 地點，進行地理編碼，並更新回資料庫。
    """
    logger.info("開始處理待地理編碼的 Yes123 地點。" )
    total_updated_locations = 0
    geocoding_cache = load_geocoding_cache()
    loop = asyncio.get_running_loop()

    now = datetime.now(timezone.utc)
    retry_failed_threshold = now - timedelta(hours=GEOCODING_RETRY_FAILED_DURATION_HOURS)

    with get_session(db_name=db_name) as session:
        # 使用 JOIN 和篩選條件來獲取總數
        # 使用 aliased 來明確 JOIN 路徑
        url_alias = aliased(Url)
        total_pending_locations = session.execute(
            select(func.count(Location.id)).
            select_from(Location).
            join(JobLocation, Location.id == JobLocation.location_id).
            join(Job, JobLocation.job_id == Job.source_job_id).
            join(url_alias, Job.url == url_alias.source_url).filter(
                (Location.latitude.is_(None)) | (Location.latitude == '') |
                (Location.longitude.is_(None)) | (Location.longitude == ''),
                ((url_alias.details_crawl_status != CrawlStatus.FAILED.value) | (url_alias.details_crawled_at < retry_failed_threshold))
            )
        ).scalar_one()
    logger.info(f"總共有 {total_pending_locations} 個地點需要地理編碼。" )

    processed_locations_count = 0

    while True:
        locations_to_process: List[Location] = []
        with get_session(db_name=db_name) as session:
            # 查詢所有 latitude 或 longitude 為 NULL 或空字串的地點，並優先處理 NULL 值
            # 使用 aliased 來明確 JOIN 路徑
            url_alias = aliased(Url)
            locations_to_process = session.execute(
                select(Location).
                select_from(Location).
                join(JobLocation, Location.id == JobLocation.location_id).
                join(Job, JobLocation.job_id == Job.source_job_id).
                join(url_alias, Job.url == url_alias.source_url).where(
                    (Location.latitude.is_(None)) | (Location.latitude == '') |
                    (Location.longitude.is_(None)) | (Location.longitude == ''),
                    ((url_alias.details_crawl_status != CrawlStatus.FAILED.value) | (url_alias.details_crawled_at < retry_failed_threshold))
                ).order_by(
                    Location.latitude.is_(None).desc(),
                    Location.longitude.is_(None).desc()
                ).limit(batch_size)
            ).scalars().unique().all()

            if not locations_to_process:
                logger.info("沒有更多需要地理編碼的 Yes123 地點。" )
                break

            logger.info(f"找到 {len(locations_to_process)} 個需要地理編碼的 Yes123 地點。" )

            tasks = []
            locations_with_cleaned_addresses = []

            for location in locations_to_process:
                if location.address_detail:
                    cleaned_address = clean_address(location.address_detail)
                    if not cleaned_address:
                        logger.warning("Address became empty after cleaning.", original_address=location.address_detail)
                        # If address becomes empty, we mark the associated URL as FAILED to prevent re-processing
                        # This requires finding the associated URL, which is complex here. For now, we skip.
                        continue
                    
                    tasks.append(loop.run_in_executor(None, partial(geocode_address, cleaned_address, geocoding_cache, True)))
                    locations_with_cleaned_addresses.append((location, cleaned_address))
                else:
                    logger.warning("地點地址為空，無法進行地理編碼。", location_id=location.id)
                    # If address is empty, we mark the associated URL as FAILED to prevent re-processing
                    # This requires finding the associated URL, which is complex here. For now, we skip.
            
            geocoding_results = await asyncio.gather(*tasks)

            updated_locations_in_batch = 0
            for i, coordinates in enumerate(geocoding_results):
                location, cleaned_address = locations_with_cleaned_addresses[i]
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
                    # If geocoding failed, we mark the associated URL as FAILED to prevent re-processing
                    # This requires finding the associated URL, which is complex here. For now, we skip.
            
            if updated_locations_in_batch > 0:
                logger.info(f"準備提交 {updated_locations_in_batch} 個地點的地理編碼更新。" )
                try:
                    session.commit()
                    save_geocoding_cache(geocoding_cache)
                    logger.info(f"已成功提交 {updated_locations_in_batch} 個地點的地理編碼更新到資料庫。" )
                    total_updated_locations += updated_locations_in_batch
                    processed_locations_count += updated_locations_in_batch
                    remaining_locations = total_pending_locations - processed_locations_count
                    logger.info(f"進度: 已處理 {processed_locations_count} 個地點，剩餘 {remaining_locations} 個地點。" )
                except Exception as e:
                    session.rollback()
                    logger.error(f"提交地理編碼更新時發生錯誤: {e}", exc_info=True)
            else:
                logger.info("此批次沒有地點的地理編碼需要更新或提交。" )
                # Removed the problematic termination condition
    
    logger.info(f"地理編碼處理完成。總共更新了 {total_updated_locations} 個地點。" )

def _run_local_test():
    db_name = os.environ.get('CRAWLER_DB_NAME', 'db_YES123')
    asyncio.run(process_pending_geocoding_yes123(db_name=db_name, batch_size=100))
    sync_job_observations_geocoding(db_name=db_name)

if __name__ == "__main__":
    _run_local_test()