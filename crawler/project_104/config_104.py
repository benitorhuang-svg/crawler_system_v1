# crawler/project_104/config.py
import structlog
from crawler.config import config_section

logger = structlog.get_logger(__name__)

# 104 平台相關設定
JOB_CAT_URL_104 = config_section.get(
    "JOB_CAT_URL_104", "https://static.104.com.tw/category-tool/json/JobCat.json"
)
JOB_API_BASE_URL_104 = config_section.get(
    "JOB_API_BASE_URL_104", "https://www.104.com.tw/job/ajax/content/"
)
WEB_NAME_104 = config_section.get("WEB_NAME_104", "104_人力銀行")

HEADERS_104 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Referer": "https://www.104.com.tw/jobs/search",
}

HEADERS_104_JOB_API = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Referer": "https://www.104.com.tw/job/",
}

HEADERS_104_URL_CRAWLER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Referer": "https://www.104.com.tw/jobs/search/",
}

URL_CRAWLER_BASE_URL_104 = config_section.get(
    "URL_CRAWLER_BASE_URL_104", "https://www.104.com.tw/jobs/search/api/jobs"
)
URL_CRAWLER_PAGE_SIZE_104 = int(config_section.get("URL_CRAWLER_PAGE_SIZE_104", "20"))
URL_CRAWLER_ORDER_BY_104 = int(
    config_section.get("URL_CRAWLER_ORDER_BY_104", "16")
)  # 16 (最近更新)
