import os

# python -m crawler.project_cakeresume.task_geocoded_cakeresume
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---

import structlog

import re
from typing import List

from sqlalchemy import select

from crawler.database.connection import get_session
from crawler.database.models import Location
from crawler.database.repository import sync_job_observations_geocoding
from crawler.geocoding.client import geocode_address
from crawler.config import get_db_name_for_platform
from crawler.database.schemas import SourcePlatform

logger = structlog.get_logger(__name__)

def clean_address(address: str) -> str:
    """Cleans the address string by removing extraneous text."""
    if not address:
        return ""
    # This is a basic cleaner, can be adapted for CakeResume's specific patterns if needed
    address = re.sub(r"[\(（【].*?[\)）】]", "", address)
    address = address.strip()
    return address

def process_pending_geocoding_cakeresume(db_name: str = None, batch_size: int = 100):
    """
    Fetches pending geocoding locations for CakeResume, processes them, and updates the database.
    """
    logger.info("Starting to process pending geocoding for CakeResume locations.")
    total_updated_locations = 0

    while True:
        with get_session(db_name=db_name) as session:
            # Query for locations needing geocoding
            locations_to_process: List[Location] = session.execute(
                select(Location).where(
                    (Location.latitude.is_(None)) | (Location.latitude == '') |
                    (Location.longitude.is_(None)) | (Location.longitude == '')
                ).limit(batch_size)
            ).scalars().all()

            if not locations_to_process:
                logger.info("No more CakeResume locations to geocode.")
                break

            logger.info(f"Found {len(locations_to_process)} CakeResume locations to geocode.")

            updated_in_batch = 0
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
                        updated_in_batch += 1
                        logger.debug(
                            "Geocoding successful and marked for update.",
                            address=cleaned_address,
                            latitude=location.latitude,
                            longitude=location.longitude,
                        )
                    else:
                        logger.warning(
                            "Geocoding failed.",
                            address=cleaned_address,
                        )
                else:
                    logger.warning(
                        "Location address is empty, cannot geocode.",
                        location_id=location.id,
                    )
            
            if updated_in_batch > 0:
                logger.info(f"Committing {updated_in_batch} geocoding updates.")
                try:
                    session.commit()
                    total_updated_locations += updated_in_batch
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error committing geocoding updates: {e}", exc_info=True)
            else:
                logger.info("No locations were updated in this batch.")
                if locations_to_process:
                    logger.warning("Geocoding failed for all locations in this batch, stopping.")
                    break
    
    logger.info(f"Geocoding process finished. Total updated locations: {total_updated_locations}.")

def _run_local_test():
    db_name = get_db_name_for_platform(SourcePlatform.PLATFORM_CAKERESUME.value)
    process_pending_geocoding_cakeresume(db_name=db_name, batch_size=10)
    sync_job_observations_geocoding(db_name=db_name)

if __name__ == "__main__":
    _run_local_test()
