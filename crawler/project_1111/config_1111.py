# crawler/project_1111/config_1111.py
import structlog
from crawler.config import config_section

logger = structlog.get_logger(__name__)

# 1111 平台相關設定
WEB_NAME_1111 = config_section.get("WEB_NAME_1111", "1111_人力銀行")
JOB_CAT_URL_1111 = config_section.get("JOB_CAT_URL_1111", "https://www.1111.com.tw/api/v1/codeCategories/")
JOB_API_BASE_URL_1111 = config_section.get("JOB_API_BASE_URL_1111", "https://www.1111.com.tw/api/v1/search/jobs/")
JOB_DETAIL_BASE_URL_1111 = config_section.get("JOB_DETAIL_BASE_URL_1111", "https://www.1111.com.tw/job/")

HEADERS_1111 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Referer": "https://www.1111.com.tw",
}

HEADERS_1111_JOB_API = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://www.1111.com.tw/search/job",
}

URL_CRAWLER_PAGE_SIZE_1111 = int(config_section.get("URL_CRAWLER_PAGE_SIZE_1111", "40"))
URL_CRAWLER_ORDER_BY_1111 = config_section.get("URL_CRAWLER_ORDER_BY_1111", "date") # "date" or "relevance"
