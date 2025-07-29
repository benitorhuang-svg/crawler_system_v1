import requests
import time
import random
from collections import deque
from crawler.worker import app # 從 worker.py 導入 Celery app 實例
from crawler.database.repository import upsert_urls # 導入資料庫儲存函數
from crawler.database.models import SourcePlatform # 導入 SourcePlatform 枚舉

# API 相關常數
BASE_URL = "https://www.104.com.tw/jobs/search/api/jobs"
PAGE_SIZE = 30
ORDER_BY_RECENT_UPDATE = 16 # 16 (最近更新)
REQUEST_TIMEOUT_SECONDS = 20 # 請求超時時間

# 假設 HEADERS 變數在模組的其他地方已經定義或從 config 導入。
# 如果沒有，您需要在此處定義或從正確的設定檔中導入。
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

@app.task
def crawl_and_store_category_urls(job_category_code: str):
    """
    Celery 任務：遍歷指定職缺類別的所有頁面，抓取職缺網址，並將其儲存到資料庫。
    """
    global_job_url_set = set() # 用於儲存所有頁面抓取到的唯一原始職缺網址
    recent_counts = deque(maxlen=4) # 連續多少頁沒有新資料則提前結束

    current_page = 1
    print(f"任務開始抓取職缺類別: {job_category_code}")

    while True:
        params = {
            'jobsource': 'm_joblist_search',
            'page': current_page,
            'pagesize': PAGE_SIZE,
            'order': ORDER_BY_RECENT_UPDATE,
            'jobcat': job_category_code,
        }

        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status() # 如果狀態碼不是 200，則拋出 HTTPError
            api_data = response.json()

            api_job_urls = api_data.get('data')
            if not isinstance(api_job_urls, list):
                print(f"API 回應 'data' 格式不正確或缺失，頁碼: {current_page}。回應: {api_data}")
                break

            for job_url_item in api_job_urls:
                job_link = job_url_item.get('link', {}).get('job')
                if job_link:
                    global_job_url_set.add(job_link)

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP 錯誤發生: {http_err}，頁碼: {current_page}")
            break
        except requests.exceptions.ConnectionError as conn_err:
            print(f"連線錯誤發生: {conn_err}，頁碼: {current_page}")
            break
        except requests.exceptions.Timeout as timeout_err:
            print(f"請求超時錯誤發生: {timeout_err}，頁碼: {current_page}")
            break
        except requests.exceptions.RequestException as req_err:
            print(f"發生未知請求錯誤: {req_err}，頁碼: {current_page}")
            break
        except ValueError: # 處理 JSON 解碼錯誤
            print(f"無法解碼 JSON 回應，頁碼: {current_page}。")
        except Exception as e: # 捕獲其他未預期的錯誤
            print(f"抓取頁碼 {current_page} 時發生未預期錯誤: {e}")

        # 檢查是否有新資料
        total_jobs = len(global_job_url_set)
        recent_counts.append(total_jobs)
        if len(recent_counts) == recent_counts.maxlen and len(set(recent_counts)) == 1:
            print(f"連續 {recent_counts.maxlen} 次沒有新資料，提前結束。")
            break

        time.sleep(random.uniform(0.5, 1.5)) # 每次請求後隨機延遲

        current_page += 1

    # 任務結束後，將所有收集到的原始網址儲存到資料庫
    if global_job_url_set:
        print(f"任務完成，開始儲存 {len(global_job_url_set)} 筆原始職缺 URL 到資料庫...")
        add_or_update_urls(list(global_job_url_set), SourcePlatform.PLATFORM_104)
    else:
        print("任務完成，沒有收集到任何 URL，跳過資料庫儲存。")

    print(f"任務 {job_category_code} 執行完畢。")

if __name__ == '__main__':
    JOBCAT_CODE = "2007000000"
    crawl_and_store_category_urls(JOBCAT_CODE)
    # 啟動 producer: python -m crawler.project_104.task_urls_104