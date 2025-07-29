import requests
import time
import random
from collections import deque
import structlog

from crawler.worker import app # 從 worker.py 導入 Celery app 實例
from crawler.database.models import SourcePlatform # 導入 SourcePlatform 枚舉
from crawler.database.repository import upsert_urls
from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

# API 相關常數
BASE_URL = "https://www.104.com.tw/jobs/search/api/jobs"
PAGE_SIZE = 20
ORDER_BY_RECENT_UPDATE = 16 # 16 (最近更新)
REQUEST_TIMEOUT_SECONDS = 20 # 請求超時時間
UPLOAD_BATCH_SIZE = 30 # 每收集 30 個 URL 就上傳一次

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Referer": "https://www.104.com.tw/",
}

@app.task
def crawl_and_store_category_urls(job_category_code: str):
    """
    Celery 任務：遍歷指定職缺類別的所有頁面，抓取職缺網址，並將其儲存到資料庫。
    """
    global_job_url_set = set() # 用於儲存所有頁面抓取到的唯一原始職缺網址
    current_batch_urls = [] # 用於儲存當前批次要上傳的 URL
    recent_counts = deque(maxlen=4) # 連續多少頁沒有新資料則提前結束

    current_page = 1
    logger.info("任務開始抓取職缺類別", job_category_code=job_category_code)

    while True:
        # 每五頁顯示一次日誌
        if current_page % 5 == 1:
            logger.info("目前頁面", page=current_page)

        params = {
            'jobsource': 'index_s',
            'page': current_page,
            'pagesize': PAGE_SIZE,
            'order': ORDER_BY_RECENT_UPDATE,
            'jobcat': job_category_code,
            'mode': 's',
            'searchJobs': '1',
        }

        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
            response.raise_for_status() # 如果狀態碼不是 200，則拋出 HTTPError
            api_data = response.json()

            api_job_urls = api_data.get('data')
            if not isinstance(api_job_urls, list):
                logger.error("API 回應 'data' 格式不正確或缺失", page=current_page, api_data=api_data)
                break

            for job_url_item in api_job_urls:
                job_link = job_url_item.get('link', {}).get('job')
                if job_link:
                    # 只有當 job_link 是新發現的才加入 current_batch_urls
                    if job_link not in global_job_url_set:
                        global_job_url_set.add(job_link)
                        current_batch_urls.append(job_link)
            
            # 檢查是否達到批次上傳大小
            if len(current_batch_urls) >= UPLOAD_BATCH_SIZE:
                logger.info("達到批次上傳大小，開始上傳", count=len(current_batch_urls))
                upsert_urls(SourcePlatform.PLATFORM_104, current_batch_urls)
                current_batch_urls.clear() # 清空暫存列表，準備收集下一批

        except requests.exceptions.HTTPError as http_err:
            logger.error("HTTP 錯誤發生", error=str(http_err), page=current_page)
            break
        except requests.exceptions.ConnectionError as conn_err:
            logger.error("連線錯誤發生", error=str(conn_err), page=current_page)
            break
        except requests.exceptions.Timeout as timeout_err:
            logger.error("請求超時錯誤發生", error=str(timeout_err), page=current_page)
            break
        except requests.exceptions.RequestException as req_err:
            logger.error("發生未知請求錯誤", error=str(req_err), page=current_page)
            break
        except ValueError: # 處理 JSON 解碼錯誤
            logger.error("無法解碼 JSON 回應", page=current_page)
        except Exception as e: # 捕獲其他未預期的錯誤
            logger.error("抓取頁碼時發生未預期錯誤", error=str(e), page=current_page, exc_info=True)

        # 檢查是否有新資料 (基於 global_job_url_set 的總數)
        total_jobs = len(global_job_url_set)
        recent_counts.append(total_jobs)
        if len(recent_counts) == recent_counts.maxlen and len(set(recent_counts)) == 1:
            logger.info("連續沒有新資料，提前結束", max_len=recent_counts.maxlen)
            break

        time.sleep(random.uniform(0.5, 1.5)) # 每次請求後隨機延遲

        current_page += 1

    # 任務結束後，將所有收集到的原始網址儲存到資料庫
    if current_batch_urls:
        logger.info("任務完成，開始儲存剩餘原始職缺 URL 到資料庫", count=len(current_batch_urls))
        upsert_urls(SourcePlatform.PLATFORM_104, current_batch_urls)
    else:
        logger.info("任務完成，沒有收集到任何 URL，跳過資料庫儲存")

    logger.info("任務執行完畢", job_category_code=job_category_code)

# if __name__ == '__main__':
#     # 啟動本地測試 task_urls_104: 
#     # APP_ENV=DEV python -m crawler.project_104.task_urls_104
#     # 2007000000 、2004003003 、2007001000、2007002000
#     JOBCAT_CODE = "2007002000"
#     crawl_and_store_category_urls(JOBCAT_CODE)