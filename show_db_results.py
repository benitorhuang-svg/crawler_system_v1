

import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from crawler.database.connection import initialize_database, get_session
from crawler.database.models import CategorySource

def main():
    """
    Connects to the database and prints all category source data.
    """
    print("--- Fetching All Category Data from test_db ---")
    initialize_database()

    with get_session() as session:
        all_categories = session.query(CategorySource).order_by(CategorySource.id).all()

        if not all_categories:
            print("No categories found in the database.")
            return

        # Print header
        print(f"{'ID':<5} {'Platform':<20} {'Category ID':<35} {'Parent ID':<30} {'Category Name'}")
        print("-" * 150)

        for category in all_categories:
            print(
                f"{str(category.id):<5} "
                f"{str(category.source_platform.value):<20} "
                f"{str(category.source_category_id):<35} "
                f"{str(category.parent_source_id):<30} "
                f"{str(category.source_category_name)}"
            )

    print("\n--- Query Complete ---")

if __name__ == "__main__":
    os.environ['CRAWLER_DB_NAME'] = 'test_db'
    main()

