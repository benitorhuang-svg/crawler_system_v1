
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from process_json_to_db import process_json_file_to_db

if __name__ == "__main__":
    file_path = "/home/soldier/crawler_system_v0_local_test/db_YES123.json"
    process_json_file_to_db(file_path)
