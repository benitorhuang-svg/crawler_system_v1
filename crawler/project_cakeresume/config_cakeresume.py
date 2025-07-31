# crawler/project_cakeresume/config_cakeresume.py
import structlog
from crawler.config import config_section

logger = structlog.get_logger(__name__)

# CakeResume 平台相關設定
WEB_NAME_CAKERESUME = config_section.get("WEB_NAME_CAKERESUME", "CakeResume")
JOB_CAT_URL_CAKERESUME = config_section.get("JOB_CAT_URL_CAKERESUME", "https://www.cakeresume.com/jobs")
JOB_LISTING_BASE_URL_CAKERESUME = config_section.get("JOB_LISTING_BASE_URL_CAKERESUME", "https://www.cakeresume.com/jobs")
logger.info("JOB_LISTING_BASE_URL_CAKERESUME loaded", url=JOB_LISTING_BASE_URL_CAKERESUME)
JOB_DETAIL_BASE_URL_CAKERESUME = config_section.get("JOB_DETAIL_BASE_URL_CAKERESUME", "https://www.cakeresume.com") # Job URLs are constructed as base + path

HEADERS_CAKERESUME = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Referer": "https://www.cakeresume.com",
}

URL_CRAWLER_PAGE_SIZE_CAKERESUME = int(config_section.get("URL_CRAWLER_PAGE_SIZE_CAKERESUME", "20")) # Not directly used by CakeResume, but for consistency
URL_CRAWLER_ORDER_BY_CAKERESUME = config_section.get("URL_CRAWLER_ORDER_BY_CAKERESUME", "latest") # "latest" or other options
