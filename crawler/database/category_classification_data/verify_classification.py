import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from crawler.database.schemas import SourcePlatform
from crawler.database.connection import initialize_database
from crawler.database.repository import get_source_categories
import structlog

# Configure logging for the script (can be removed if only using print)
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
    print("Initializing database connection...")
    initialize_database()
    print("Database initialized.")

    platform_to_check = SourcePlatform.PLATFORM_104
    categories_to_check = [
        "資訊軟體系統類",
        "經營／人資類",
        "財會／金融專業類",
        "其他職類",
    ]

    print(f"Verifying parent_source_id for {platform_to_check.value} categories...")

    # Fetch all categories for the platform and then filter by name
    all_platform_categories = get_source_categories(platform_to_check)
    
    found_categories = {}
    for cat in all_platform_categories:
        found_categories[cat.source_category_name] = cat

    for category_name in categories_to_check:
        category = found_categories.get(category_name)
        if category:
            print(
                f"Category: {category.source_category_name}, "
                f"Source ID: {category.source_category_id}, "
                f"Parent Source ID: {category.parent_source_id}"
            )
        else:
            print(f"Category '{category_name}' not found for {platform_to_check.value}.")

    print("Verification complete.")

if __name__ == "__main__":
    main()