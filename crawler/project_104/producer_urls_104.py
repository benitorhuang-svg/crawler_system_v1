from crawler.project_104.task_jobs_104 import fetch_url_data_104
from crawler.database.get_category_ids import get_source_category_ids
import structlog

logger = structlog.get_logger(__name__)

# 獲取類別數據的 DataFrame
df_categories = get_source_category_ids()  

logger.info("Filtering urls by specific parent_id")
target_parent_id = '2001000000'  

# 篩選出指定 parent_id 的所有子類別
filtered_categories = df_categories[df_categories['parent_source_id'] == target_parent_id]

if not filtered_categories.empty:
    logger.info(f"Sub-categories for parent_id {target_parent_id}:")
    for index, row in filtered_categories.iterrows():
        logger.info("Sub-category", category_id=row['source_category_id'], category_name=row['source_category_name'])
else:
    logger.info(f"No sub-categories found for parent_id {target_parent_id}.")

if __name__ == '__main__':
    JOBCAT_CODE = "2007000000"
    # 在運行此程式碼之前，請確保 Celery worker 和 RabbitMQ/Redis 服務正在運行
    # 啟動 worker: celery -A crawler.worker worker -l info -Q urls_104
    # 啟動 producer: python -m crawler.project_104.task_urls_104
    # 啟動 producer: python -m crawler.project_104.producer_urls_104
    print(filtered_categories)

    # produce_104_urls(JOBCAT_CODE)
