import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from crawler.database.schemas import SourcePlatform
from crawler.database.connection import initialize_database
from crawler.database.repository import get_root_categories, update_category_parent_id
import structlog
import json

# Configure logging for the script
structlog.configure(
    processors=[
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger(__name__)

# 15 項大項目歸類定義
# 從 JSON 檔案載入 MAJOR_CATEGORIES
_major_categories_file_path = os.path.join(os.path.dirname(__file__), "major_categories.json")
with open(_major_categories_file_path, 'r', encoding='utf-8') as f:
    MAJOR_CATEGORIES = json.load(f)

# 映射關係：原始分類名稱 -> 大項目歸類 ID
# 從 JSON 檔案載入 MAPPING
_mapping_file_path = os.path.join(os.path.dirname(__file__), "mapping.json")
with open(_mapping_file_path, 'r', encoding='utf-8') as f:
    _raw_mapping = json.load(f)

MAPPING = {
    SourcePlatform(platform_name): platform_map
    for platform_name, platform_map in _raw_mapping.items()
}

# def main():
#     logger.info("Initializing database connection...")
#     initialize_database()
#     logger.info("Database initialized.")

#     # 1. 插入新的大項目歸類 (This step is removed to prevent data deletion)
#     # logger.info("Inserting major classification categories...")
#     # sync_source_categories(SourcePlatform.PLATFORM_104, MAJOR_CATEGORIES)
#     # logger.info("Major classification categories inserted.")

#     # 2. 更新現有頂層分類的父 ID
#     platforms_to_process = [
#         SourcePlatform.PLATFORM_104,
#         SourcePlatform.PLATFORM_1111,
#         SourcePlatform.PLATFORM_CAKERESUME,
#         SourcePlatform.PLATFORM_YES123,
#     ]

#     major_category_ids = {cat["source_category_id"] for cat in MAJOR_CATEGORIES}

#     for platform in platforms_to_process:
#         logger.info(f"Processing platform: {platform.value}")
#         root_categories = get_root_categories(platform)
        
#         for category in root_categories:
#             # Skip updating the major categories themselves
#             if category.source_category_id in major_category_ids:
#                 continue

#             original_category_name = category.source_category_name.strip()
#             new_parent_id = MAPPING[platform].get(original_category_name)

#             if new_parent_id:
#                 logger.info(
#                     f"Updating parent_source_id for {platform.value} - {original_category_name} to {new_parent_id}"
#                 )
#                 update_category_parent_id(
#                     platform,
#                     category.source_category_id,
#                     new_parent_id
#                 )
#             else:
#                 logger.warning(
#                     f"No mapping found for {platform.value} - {original_category_name}. Skipping update."
#                 )
#     logger.info("Classification application complete.")

# if __name__ == "__main__":
#     main()
