import os
import sys
import json

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from crawler.database.schemas import SourcePlatform
from crawler.database.connection import initialize_database
from crawler.database.repository import get_root_categories
import structlog

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

def main():
    logger.info("Initializing database connection...")
    initialize_database()
    logger.info("Database initialized.")

    platforms = [
        SourcePlatform.PLATFORM_104,
        SourcePlatform.PLATFORM_1111,
        SourcePlatform.PLATFORM_CAKERESUME,
        SourcePlatform.PLATFORM_YES123,
    ]

    all_root_categories = {}

    for platform in platforms:
        logger.info(f"Fetching root categories for platform: {platform.value}")
        root_categories = get_root_categories(platform)
        all_root_categories[platform.value] = [
            cat.source_category_name for cat in root_categories
        ]
        logger.info(f"Found {len(root_categories)} root categories for {platform.value}")

    output_file = os.path.join(os.path.dirname(__file__), "root_categories.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_root_categories, f, ensure_ascii=False, indent=4)
    logger.info(f"Root categories saved to {output_file}")

if __name__ == "__main__":
    main()
