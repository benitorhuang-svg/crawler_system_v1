import structlog
import requests
import json
import os
from typing import Optional, Dict

from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

ARCGIS_GEOCODE_API_URL = "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates"
GEOCODING_CACHE_FILE = os.path.join(os.path.dirname(__file__), "geocoding_cache.json")

def _load_cache() -> Dict[str, Dict[str, float]]:
    if os.path.exists(GEOCODING_CACHE_FILE):
        with open(GEOCODING_CACHE_FILE, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Geocoding cache file is corrupted, starting with empty cache.")
                return {}
    return {}

def _save_cache(cache: Dict[str, Dict[str, float]]):
    with open(GEOCODING_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=4)

def geocode_address(address: str) -> Optional[Dict[str, float]]:
    """
    呼叫 ArcGIS 地理編碼服務，將地址轉換為經緯度，並使用檔案快取。
    """
    cache = _load_cache()
    if address in cache:
        logger.info("Geocoding result found in cache.", address=address)
        return cache[address]

    logger.info("Attempting to geocode address using ArcGIS API.", address=address)

    params = {
        "SingleLine": address,
        "f": "json",
        "outFields": "maxLocations=1"
    }

    try:
        response = requests.get(ARCGIS_GEOCODE_API_URL, params=params)
        response.raise_for_status()  # 如果請求不成功，則拋出 HTTPError

        data = response.json()

        if data and "candidates" in data and len(data["candidates"]) > 0:
            first_candidate = data["candidates"][0]
            if "location" in first_candidate:
                location = first_candidate["location"]
                if "x" in location and "y" in location:
                    longitude = location["x"]
                    latitude = location["y"]
                    result = {"latitude": latitude, "longitude": longitude}
                    cache[address] = result
                    _save_cache(cache)
                    logger.info(
                        "Geocoding successful and cached.",
                        address=address,
                        latitude=latitude,
                        longitude=longitude
                    )
                    return result
        
        logger.warning("Geocoding failed: No valid candidates found or location data missing.", address=address, response_data=data)
        cache[address] = None # Cache None to avoid repeated failed lookups
        _save_cache(cache)
        return None

    except requests.exceptions.RequestException as e:
        logger.error("Error during geocoding API request.", address=address, error=e)
        cache[address] = None # Cache None for network errors as well
        _save_cache(cache)
        return None
    except ValueError as e:
        logger.error("Error parsing geocoding API response.", address=address, error=e)
        cache[address] = None # Cache None for parsing errors
        _save_cache(cache)
        return None
