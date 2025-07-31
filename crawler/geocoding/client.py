import structlog
from typing import Optional, Dict
import random
import time

from crawler.config import (
    URL_CRAWLER_SLEEP_MIN_SECONDS,
    URL_CRAWLER_SLEEP_MAX_SECONDS,
)
from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

def geocode_address(address: str) -> Optional[Dict[str, float]]:
    """
    模擬地理編碼服務，將地址轉換為經緯度。
    在實際應用中，這裡會呼叫第三方地理編碼 API (例如 Google Maps Geocoding API)。
    """
    logger.info("Attempting to geocode address.", address=address)
    
    # 模擬網路延遲
    sleep_time = random.uniform(
        URL_CRAWLER_SLEEP_MIN_SECONDS, URL_CRAWLER_SLEEP_MAX_SECONDS
    )
    time.sleep(sleep_time)

    # 簡單的模擬經緯度，實際應根據地址返回真實數據
    if "台北" in address:
        return {"latitude": 25.032969, "longitude": 121.564559} # 台北市政府
    elif "台中" in address:
        return {"latitude": 24.137135, "longitude": 120.687138} # 台中市政府
    elif "高雄" in address:
        return {"latitude": 22.627278, "longitude": 120.301435} # 高雄市政府
    else:
        logger.warning("Address not found in mock geocoding service.", address=address)
        return None
