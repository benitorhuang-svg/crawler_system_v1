import os

# #  python -m crawler.project_1111.task_urls_1111
# # --- Local Test Environment Setup ---
# if __name__ == "__main__":
#     os.environ['CRAWLER_DB_NAME'] = 'test_db'
# # --- End Local Test Environment Setup ---


import structlog
import time
import requests
import concurrent.futures
from typing import List, Optional, Dict, Any, Set

from crawler.worker import app
from crawler.database.connection import initialize_database
from crawler.database.schemas import (
    SourcePlatform,
    CategorySourcePydantic,
    JobPydantic,
    UrlPydantic,
    JobObservationPydantic,
)
from crawler.database.repository import (
    upsert_urls,
    upsert_url_categories,
    upsert_jobs,
    get_all_categories_for_platform,
    insert_job_observations,
)
from crawler.project_1111.client_1111 import fetch_job_urls_from_1111_api, fetch_job_detail_html_from_1111
from crawler.project_1111.parser_apidata_1111 import parse_job_list_json_to_pydantic, parse_job_detail_html_to_pydantic
from crawler.config import (
    URL_CRAWLER_UPLOAD_BATCH_SIZE,
    get_db_name_for_platform,
    URL_CRAWLER_API_RETRIES,
    URL_CRAWLER_API_BACKOFF_FACTOR,
)
from crawler.project_1111.config_1111 import (
    URL_CRAWLER_ORDER_BY_1111,
)

logger = structlog.get_logger(__name__)


def _get_db_name(db_name_override: Optional[str]) -> str:
    """根據覆寫值或環境變數決定要使用的資料庫名稱。"""
    if db_name_override:
        return db_name_override
    return os.environ.get('CRAWLER_DB_NAME') or get_db_name_for_platform(SourcePlatform.PLATFORM_1111.value)

def _create_observation_from_job(job: JobPydantic) -> JobObservationPydantic:
    """從 JobPydantic 物件建立 JobObservationPydantic 物件的輔助函式。"""
    job_dict = job.model_dump()
    # 處理巢狀或特殊欄位
    job_dict['company_id'] = job.company.source_company_id if job.company else None
    job_dict['company_name'] = job.company.name if job.company else None
    job_dict['company_url'] = job.company.url if job.company else None
    if job.locations:
        job_dict['location_text'] = job.locations[0].address_detail
        job_dict['region'] = job.locations[0].region
        job_dict['district'] = job.locations[0].district
        job_dict['latitude'] = job.locations[0].latitude
        job_dict['longitude'] = job.locations[0].longitude
    else:
        job_dict['location_text'] = None
        job_dict['region'] = None
        job_dict['district'] = None
        job_dict['latitude'] = None
        job_dict['longitude'] = None
    
    # 將 skills 列表轉換為逗號分隔的字串，如果為空則為 None
    job_dict['skills'] = ", ".join([skill.name for skill in job.skills]) if job.skills else None
    
    # 移除 JobPydantic 有但 JobObservationPydantic 沒有的欄位
    job_dict.pop('company', None)
    job_dict.pop('locations', None)

    return JobObservationPydantic(**job_dict)

def _upsert_batch_to_db(
    jobs_to_upsert: List[JobPydantic],
    observations_to_insert: List[JobObservationPydantic],
    url_category_tags: List[Dict[str, str]],
    db_name: str
):
    """將收集到的批次資料寫入資料庫。"""
    if not observations_to_insert:
        logger.info("此批次無資料可上傳。", db_name=db_name, category=observations_to_insert[0].source_platform if observations_to_insert else "N/A")
        return

    # 優先插入觀測資料
    insert_job_observations(observations_to_insert, db_name=db_name)
    logger.info(f"成功插入 {len(observations_to_insert)} 筆職缺觀測記錄。", db_name=db_name, category=observations_to_insert[0].source_platform if observations_to_insert else "N/A")

    # 如果有新職缺，則更新相關表格
    if jobs_to_upsert:
        urls_to_upsert = [
            UrlPydantic(source_url=job.url, source=job.source_platform)
            for job in jobs_to_upsert
        ]
        upsert_jobs(jobs_to_upsert, db_name=db_name)
        upsert_urls(SourcePlatform.PLATFORM_1111, urls_to_upsert, db_name=db_name)
        logger.info(
            "成功更新批次資料至資料庫。",
            jobs_upserted=len(jobs_to_upsert),
            urls_upserted=len(urls_to_upsert),
            db_name=db_name,
            category=jobs_to_upsert[0].source_platform if jobs_to_upsert else "N/A"
        )
    
    # Always upsert category tags if there are any
    if url_category_tags:
        upsert_url_categories(url_category_tags, db_name=db_name)
        logger.info(f"成功更新 {len(url_category_tags)} 筆職缺類別標籤。", db_name=db_name, category=url_category_tags[0]["category_source_id"] if url_category_tags else "N/A")

class CategoryCrawler:
    """封裝單一職缺類別的完整爬取邏輯。"""

    def __init__(self, category: CategorySourcePydantic, db_name: str, url_limit: int, global_url_set: Set[str]):
        self.category = category
        self.db_name = db_name
        self.url_limit = url_limit
        self.global_url_set = global_url_set
        self.local_url_set: Set[str] = set()
        self.session = requests.Session()  # 為所有請求重複使用同一個 Session
        self.jobs_for_upsert: List[JobPydantic] = []
        self.jobs_for_observations: List[JobObservationPydantic] = []
        self.job_category_tags_to_upsert: List[Dict[str, str]] = []

    def run(self):
        """執行爬取任務。"""
        logger.info(
            "開始爬取 1111 職缺類別。",
            category_name=self.category.source_category_name,
            category_id=self.category.source_category_id,
            platform=SourcePlatform.PLATFORM_1111,
        )
        
        # 1. 抓取第一頁以獲取總頁數
        first_page_data = self._fetch_list_page(1)
        if not first_page_data:
            logger.error("抓取第一頁失敗，無法繼續爬取。", category=self.category.source_category_id)
            return

        total_pages = first_page_data.get("result", {}).get("pagination", {}).get("totalPage", 1)
        logger.info("從 API 取得總頁數。", total_pages=total_pages, category=self.category.source_category_id)

        # 2. 處理第一頁的結果
        self._process_page_results(first_page_data)

        # 3. 使用 ThreadPoolExecutor 平行處理剩餘頁面
        pages_to_fetch = range(2, total_pages + 1)
        if pages_to_fetch:
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                future_to_page = {
                    executor.submit(self._fetch_list_page, page_num): page_num
                    for page_num in pages_to_fetch
                }

                for future in concurrent.futures.as_completed(future_to_page):
                    if self.url_limit > 0 and len(self.global_url_set) >= self.url_limit:
                        logger.info("已達到 URL 數量上限，停止提交新任務。")
                        # 取消尚未完成的任務
                        for f in future_to_page:
                            if not f.done():
                                f.cancel()
                        break
                    
                    page_num = future_to_page[future]
                    try:
                        page_data = future.result()
                        if page_data:
                            self._process_page_results(page_data)
                    except concurrent.futures.CancelledError:
                        logger.warning("任務被取消。", page=page_num)
                    except Exception as exc:
                        logger.error("處理頁面時發生錯誤。", page=page_num, error=str(exc), exc_info=True)
        
        # 4. 提交最後剩餘的批次
        self._commit_batch()
        logger.info("類別爬取完成。", category=self.category.source_category_id)

    def _fetch_list_page(self, page_num: int, retries: int = URL_CRAWLER_API_RETRIES, backoff_factor: float = URL_CRAWLER_API_BACKOFF_FACTOR) -> Optional[Dict[str, Any]]:
        """從 1111 API 抓取單一職缺列表頁面，並包含重試機制。"""
        for attempt in range(retries):
            try:
                # 使用 self.session 進行請求
                api_response = fetch_job_urls_from_1111_api(
                    KEYWORDS="",
                    CATEGORY=self.category.source_category_id,
                    ORDER=URL_CRAWLER_ORDER_BY_1111,
                    PAGE_NUM=page_num,
                    session=self.session
                )
                return api_response
            except Exception as e:
                logger.warning("API 請求失敗，正在重試...", attempt=attempt + 1, error=str(e), page=page_num, category=self.category.source_category_id)
                if attempt < retries - 1:
                    time.sleep(backoff_factor * (2 ** attempt))
        logger.error("API 請求在多次重試後仍然失敗。", page=page_num, category=self.category.source_category_id)
        return None

    def _process_page_results(self, api_response: Dict[str, Any]):
        """處理從 API 獲得的一頁職缺列表。"""
        job_items_raw = api_response.get("result", {}).get("hits", [])
        if not job_items_raw:
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as detail_executor:
            future_to_job = {
                detail_executor.submit(self._fetch_and_parse_detail, job_raw): job_raw
                for job_raw in job_items_raw
            }

            for future in concurrent.futures.as_completed(future_to_job): # Corrected from future_to_page
                try:
                    job_pydantic = future.result()
                    if not job_pydantic:
                        continue

                    # 1. 無論如何都新增至觀測列表
                    self.jobs_for_observations.append(_create_observation_from_job(job_pydantic))

                    # Add category tags for all jobs (observations)
                    # Prioritize job_pydantic.category_tags if available, otherwise use the category of the current crawl
                    if job_pydantic.source_job_id:
                        if job_pydantic.category_tags:
                            for cat_id in job_pydantic.category_tags:
                                self.job_category_tags_to_upsert.append({
                                    "job_id": job_pydantic.source_job_id,
                                    "category_source_id": cat_id,
                                })
                        elif self.category.source_category_id:
                            self.job_category_tags_to_upsert.append({
                                "job_id": job_pydantic.source_job_id,
                                "category_source_id": self.category.source_category_id,
                            })

                    # Add to upsert list only if unique within this category crawl
                    if job_pydantic.source_job_id not in self.local_url_set:
                        self.local_url_set.add(job_pydantic.source_job_id)
                        self.global_url_set.add(job_pydantic.source_job_id)
                        self.jobs_for_upsert.append(job_pydantic)
                        logger.debug("Added new job to upsert list.", url=job_pydantic.url, job_id=job_pydantic.source_job_id, category=self.category.source_category_id)
                    else:
                        logger.debug("Skipping duplicate job for upsert.", url=job_pydantic.url, job_id=job_pydantic.source_job_id, category=self.category.source_category_id)

                    # 3. 檢查是否達到批次上傳的門檻 (以觀測數量為準)
                    if len(self.jobs_for_observations) >= URL_CRAWLER_UPLOAD_BATCH_SIZE:
                        self._commit_batch()

                except Exception as exc:
                    logger.error("處理單一職缺時發生錯誤。", error=exc, exc_info=True, category=self.category.source_category_id)

    def _fetch_and_parse_detail(self, job_item_raw: Dict[str, Any]) -> Optional[JobPydantic]:
        """抓取並解析單一職缺的詳細頁面。"""
        # 先從列表 API 的資訊解析基礎 JobPydantic 物件
        job_pydantic = parse_job_list_json_to_pydantic(job_item_raw)
        if not job_pydantic:
            return None
        try:
            # 使用 self.session 抓取詳細頁面
            detail_html = fetch_job_detail_html_from_1111(job_pydantic.url, session=self.session)
            if detail_html:
                # 使用詳細頁面的 HTML 更新 JobPydantic 物件
                updated_job = parse_job_detail_html_to_pydantic(detail_html, job_pydantic.url, existing_job=job_pydantic)
                return updated_job or job_pydantic # 如果解析失敗，回傳原始物件
            logger.warning("抓取職缺詳細頁 HTML 失敗，將使用列表頁資料。", url=job_pydantic.url, category=self.category.source_category_id)
        except Exception as e:
            logger.error("抓取或解析職缺詳細頁時發生錯誤。", url=job_pydantic.url, error=e, category=self.category.source_category_id)
        
        return job_pydantic # 如果過程中發生任何錯誤，回傳從列表頁解析的資料

    def _commit_batch(self):
        """提交當前批次的資料到資料庫並清空批次。"""
        if not self.jobs_for_observations:
            return

        logger.info(f"達到批次大小，開始上傳 {len(self.jobs_for_observations)} 筆資料。", category=self.category.source_category_id)
        
        _upsert_batch_to_db(
            jobs_to_upsert=self.jobs_for_upsert,
            observations_to_insert=self.jobs_for_observations,
            url_category_tags=self.job_category_tags_to_upsert,
            db_name=self.db_name
        )
        
        # 清空批次
        self.jobs_for_upsert.clear()
        self.jobs_for_observations.clear()
        self.job_category_tags_to_upsert.clear()


@app.task
def crawl_and_store_1111_category_urls(job_category: dict, url_limit: int = 0, db_name_override: Optional[str] = None) -> int:
    """
    Celery 任務：爬取指定的 1111 職缺類別，並將資料儲存到資料庫。
    返回此次任務收集到的新職缺數量。
    """
    job_category_pydantic = CategorySourcePydantic.model_validate(job_category)
    db_name = _get_db_name(db_name_override)
    
    # 在 Celery 環境中，global_url_set 應為空集合，因為每個任務是獨立的。
    # 如果需要在多個任務間共享狀態，需要使用 Redis 或類似的外部儲存。
    # 為了與本地端執行邏輯保持一致，此處我們假設它是獨立的。
    global_url_set = set()

    crawler = CategoryCrawler(
        category=job_category_pydantic,
        db_name=db_name,
        url_limit=url_limit,
        global_url_set=global_url_set
    )
    crawler.run()

    collected_count = len(global_url_set)
    logger.info("任務執行完畢。", job_category_code=job_category_pydantic.source_category_id, total_collected=collected_count)
    return collected_count


if __name__ == "__main__":
    db_name_for_local_run = _get_db_name(None)
    initialize_database(db_name=db_name_for_local_run)
    logger.info(f"本地測試模式：使用資料庫 '{db_name_for_local_run}'")

    all_categories = get_all_categories_for_platform(SourcePlatform.PLATFORM_1111, db_name=db_name_for_local_run)
    
    if not all_categories:
        logger.warning("資料庫中找不到 1111 平台的類別資料，請先執行 producer_category_1111。")
    else:
        # 建立一個全域集合，用於在所有類別的爬取過程中追蹤唯一的職缺 URL
        all_collected_job_urls = set()

        # Identify all parent category IDs
        parent_category_ids = {cat.parent_source_id for cat in all_categories if cat.parent_source_id is not None}

        # Filter out categories that are parents themselves
        # Keep only categories whose source_category_id is NOT in the set of parent_category_ids
        filtered_categories = [cat for cat in all_categories if cat.source_category_id not in parent_category_ids]

        # --- 類別排序邏輯 ---
        # major_categories_path = "/home/soldier/crawler_system_v0_local_test/crawler/utils/skill_data/source_data/major_categories.json"
        # try:
        #     with open(major_categories_path, 'r', encoding='utf-8') as f:
        #         major_categories_data = json.load(f)
        #     PARENT_ORDER_LIST = [item['source_category_id'] for item in major_categories_data]
        # except (FileNotFoundError, json.JSONDecodeError) as e:
        #     logger.error(f"無法載入或解析 major_categories.json: {e}。將使用預設順序。")
        PARENT_ORDER_LIST = [] # Force empty list if major_categories_path is commented out

        # Create a mapping for parent_id to its order in PARENT_ORDER_LIST
        parent_order_map = {parent_id: i for i, parent_id in enumerate(PARENT_ORDER_LIST)}

        def custom_category_sort_key(category):
            # Priority 0: parent_source_id starts with '140'
            if category.parent_source_id and str(category.parent_source_id).startswith('140'):
                return (0, category.source_category_name)

            # Priority 1: parent_source_id in PARENT_ORDER_LIST
            if category.parent_source_id in parent_order_map:
                return (1, parent_order_map[category.parent_source_id], category.source_category_name)
            
            # Priority 2: All other categories
            return (2, category.source_category_name)

        # Apply the custom sort to all filtered categories directly
        sorted_categories = sorted(filtered_categories, key=custom_category_sort_key)
        # --- 排序邏輯結束 ---

        start_time = time.time()
        for category_pydantic in sorted_categories:
            logger.info("開始本地測試爬取任務。", category=category_pydantic.source_category_id)
            
            # 建立並執行爬蟲
            crawler = CategoryCrawler(
                category=category_pydantic,
                db_name=db_name_for_local_run,
                url_limit=0, # 本地測試不設上限
                global_url_set=all_collected_job_urls # 傳入全域集合
            )
            crawler.run()
            
            logger.info(
                "完成一個類別的爬取。",
                category=category_pydantic.source_category_id,
                total_unique_so_far=len(all_collected_job_urls)
            )
            
        end_time = time.time()
        logger.info(
            "所有類別處理完畢。",
            total_unique_jobs=len(all_collected_job_urls),
            total_time=f"{end_time - start_time:.2f} 秒"
        )