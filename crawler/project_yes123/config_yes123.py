import structlog
from crawler.config import config_section

logger = structlog.get_logger(__name__)

# yes123 平台相關設定
WEB_NAME_YES123 = config_section.get("WEB_NAME_YES123", "yes123_求職網")
JOB_CAT_URL_YES123 = config_section.get("JOB_CAT_URL_YES123", "https://www.yes123.com.tw/admin/job_refer_data.asp?sno=20090101_jobkind_001")
JOB_LISTING_BASE_URL_YES123 = config_section.get("JOB_LISTING_BASE_URL_YES123", "https://www.yes123.com.tw/findjob/job_list.asp")
JOB_DETAIL_BASE_URL_YES123 = config_section.get("JOB_DETAIL_BASE_URL_YES123", "https://www.yes123.com.tw/findjob/job_detail.asp")

HEADERS_YES123 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Referer": "https://www.yes123.com.tw",
}

URL_CRAWLER_PAGE_SIZE_YES123 = int(config_section.get("URL_CRAWLER_PAGE_SIZE_YES123", "20"))
URL_CRAWLER_ORDER_BY_YES123 = config_section.get("URL_CRAWLER_ORDER_BY_YES123", "new") # "new" or other options
