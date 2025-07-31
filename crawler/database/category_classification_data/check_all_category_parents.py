import os
import sys
import json

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from crawler.database.schemas import SourcePlatform
from crawler.database.connection import initialize_database
from crawler.database.repository import get_source_categories

from .apply_classification import MAPPING, MAJOR_CATEGORIES

def main():
    print("Initializing database connection...")
    initialize_database()
    print("Database initialized.")

    platforms_to_check = [
        SourcePlatform.PLATFORM_104,
        SourcePlatform.PLATFORM_1111,
        SourcePlatform.PLATFORM_CAKERESUME,
        SourcePlatform.PLATFORM_YES123,
    ]

    # Load original root categories from JSON for reference
    root_categories_json_path = os.path.join(os.path.dirname(__file__), "root_categories.json")
    with open(root_categories_json_path, 'r', encoding='utf-8') as f:
        original_root_categories_data = json.load(f)

    print("\n--- Detailed Category Parent ID Check ---")

    # Create a set of major category IDs for quick lookup
    major_category_ids = {cat["source_category_id"] for cat in MAJOR_CATEGORIES}

    for platform in platforms_to_check:
        print(f"\nPlatform: {platform.value}")
        all_platform_categories = get_source_categories(platform)
        
        if not all_platform_categories:
            print("  No categories found for this platform.")
            continue

        # Get original root category names for this platform from the JSON file
        original_names_for_platform = set(original_root_categories_data.get(platform.value, []))

        # Sort categories for consistent output
        sorted_categories = sorted(all_platform_categories, key=lambda x: x.source_category_name)

        for category in sorted_categories:
            status_message = ""
            
            # Check if it's one of our new major categories
            if category.source_category_id in major_category_ids:
                status_message = "(New Major Category)"

            if category.parent_source_id is None:
                if category.source_category_name in original_names_for_platform and category.source_category_id not in major_category_ids:
                    # This category was originally a root category and should have been mapped
                    expected_parent = MAPPING[platform].get(category.source_category_name)
                    if expected_parent:
                        status_message = f"(ERROR: Expected to map to {expected_parent} but parent is NULL)"
                    else:
                        status_message = "(WARNING: Original root, NULL parent, but no mapping found in MAPPING dict)"
                elif category.source_category_id not in major_category_ids:
                    status_message = "(WARNING: Unexpected NULL parent - not an original root or major category)"
            
            print(
                f"  Category: {category.source_category_name:<30} "
                f"Source ID: {category.source_category_id:<15} "
                f"Parent Source ID: {str(category.parent_source_id):<20} "
                f"{status_message}"
            )
    print("\n--- Check complete ---")

if __name__ == "__main__":
    main()
