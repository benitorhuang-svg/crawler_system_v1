import os

# python -m crawler.project_cakeresume.task_geocoded_cakeresume
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import structlog
import re
from typing import List

from sqlalchemy import select, func

from crawler.database.connection import get_session
from crawler.database.models import Location
from crawler.database.repository import sync_job_observations_geocoding
from crawler.geocoding.client import geocode_address, load_geocoding_cache, save_geocoding_cache
from crawler.config import get_db_name_for_platform, GEOCODING_RETRY_FAILED_DURATION_HOURS
from crawler.database.schemas import SourcePlatform
import asyncio
from functools import partial
from datetime import datetime, timedelta, timezone

logger = structlog.get_logger(__name__)

def clean_address(address: str) -> str:
    """Cleans the address string by removing extraneous text and irrelevant details."""
    if not address:
        return ""

    # 移除括號和括號內的所有內容
    address = re.sub(r"[\\(（【].*?[\\)）】]", "", address)
    # 移除「上班地點:」等前綴
    address = re.sub(r"^.*?上班地點:?", "", address)
    # 移除特殊符號和多餘的空格
    address = re.sub(r"[️]", "", address).strip()

    return address


async def process_pending_geocoding_cakeresume(db_name: str = None, batch_size: int = 100):
    """
    Fetches pending geocoding locations for CakeResume, processes them, and updates the database.
    """
    logger.info("Starting to process pending geocoding for CakeResume locations.")
    total_updated_locations = 0
    geocoding_cache = load_geocoding_cache()
    loop = asyncio.get_running_loop()

    now = datetime.now(timezone.utc)
    retry_failed_threshold = now - timedelta(hours=GEOCODING_RETRY_FAILED_DURATION_HOURS)

    with get_session(db_name=db_name) as session:
        # 簡化查詢，只針對 Location 表的 NULL 值和 UNGEOCODABLE 標記
        total_pending_locations = session.execute(
            select(func.count(Location.id)).
            select_from(Location).filter(
                (Location.latitude.is_(None)) | (Location.latitude == '') |
                (Location.longitude.is_(None)) | (Location.longitude == ''),
                Location.latitude != 'UNGEOCODABLE' # Exclude UNGEOCODABLE
            )
        ).scalar_one()
    logger.info(f"總共有 {total_pending_locations} 個地點需要地理編碼。")

    processed_locations_count = 0

    while True:
        locations_to_process: List[Location] = []
        with get_session(db_name=db_name) as session:
            # 簡化查詢，只針對 Location 表的 NULL 值和 UNGEOCODABLE 標記
            locations_to_process = session.execute(
                select(Location).where(
                    (Location.latitude.is_(None)) | (Location.latitude == '') |
                    (Location.longitude.is_(None)) | (Location.longitude == ''),
                    Location.latitude != 'UNGEOCODABLE' # Exclude UNGEOCODABLE
                ).order_by(
                    Location.latitude.is_(None).desc(),
                    Location.longitude.is_(None).desc()
                ).limit(batch_size)
            ).scalars().all()

            if not locations_to_process:
                logger.info("No more CakeResume locations to geocode.")
                break

            logger.info(f"Found {len(locations_to_process)} CakeResume locations to geocode.")

            tasks = []
            locations_with_cleaned_addresses = []

            for location in locations_to_process:
                if location.address_detail:
                    cleaned_address = clean_address(location.address_detail)
                    if not cleaned_address:
                        logger.warning("Address became empty after cleaning. Marking as UNGEOCODABLE.", original_address=location.address_detail)
                        location.latitude = 'UNGEOCODABLE'
                        location.longitude = 'UNGEOCODABLE'
                        # No need to add to tasks, as it's already marked
                        continue
                    
                    tasks.append(loop.run_in_executor(None, partial(geocode_address, cleaned_address, geocoding_cache, True)))
                    locations_with_cleaned_addresses.append((location, cleaned_address))
                else:
                    logger.warning("Location address is empty, cannot geocode. Marking as UNGEOCODABLE.", location_id=location.id)
                    location.latitude = 'UNGEOCODABLE'
                    location.longitude = 'UNGEOCODABLE'
            
            geocoding_results = await asyncio.gather(*tasks)

            updated_in_batch = 0
            for i, coordinates in enumerate(geocoding_results):
                location, cleaned_address = locations_with_cleaned_addresses[i]
                if coordinates and coordinates.get('latitude') != 'UNGEOCODABLE': # Check for our specific failed marker
                    location.latitude = str(coordinates["latitude"])
                    location.longitude = str(coordinates["longitude"])
                    updated_in_batch += 1
                    logger.debug(
                        "地理編碼成功並標記更新。",
                        address=cleaned_address,
                        latitude=location.latitude,
                        longitude=location.longitude,
                    )
                else:
                    # If geocoding_address returned UNGEOCODABLE, it's already set in the location object
                    # If it returned None (which it shouldn't now with UNGEOCODABLE marker), we also mark it
                    if not coordinates or coordinates.get('latitude') == 'UNGEOCODABLE':
                        location.latitude = 'UNGEOCODABLE'
                        location.longitude = 'UNGEOCODABLE'
                    logger.warning(
                        "地理編碼失敗。",
                        address=cleaned_address,
                    )
                updated_in_batch += 1 # Ensure all processed locations are counted for commit
            
            if updated_in_batch > 0:
                logger.info(f"Committing {updated_in_batch} geocoding updates.")
                try:
                    session.commit()
                    save_geocoding_cache(geocoding_cache)
                    logger.info(f"已成功提交 {updated_in_batch} 個地點的地理編碼更新到資料庫。")
                    total_updated_locations += updated_in_batch
                    processed_locations_count += updated_in_batch
                    remaining_locations = total_pending_locations - processed_locations_count
                    logger.info(f"進度: 已處理 {processed_locations_count} 個地點，剩餘 {remaining_locations} 個地點。")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error committing geocoding updates: {e}", exc_info=True)
            else:
                logger.info("No locations were updated in this batch.")
                # Removed the problematic termination condition
    
    logger.info(f"Geocoding process finished. Total updated locations: {total_updated_locations}.")

def _run_local_test():
    db_name = get_db_name_for_platform(SourcePlatform.PLATFORM_CAKERESUME.value)
    asyncio.run(process_pending_geocoding_cakeresume(db_name=db_name, batch_size=100))
    sync_job_observations_geocoding(db_name=db_name)

if __name__ == "__main__":
    _run_local_test()
