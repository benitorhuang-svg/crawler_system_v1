import os
import json
from crawler.database.connection import get_session
from crawler.database.schemas import SourcePlatform
from crawler.database.models import CategorySource

os.environ['CRAWLER_DB_NAME'] = 'test_db'

updated_categories = []
try:
    with get_session() as session:
        results = session.query(CategorySource.source_category_name, CategorySource.parent_source_id).filter(
            CategorySource.source_platform == SourcePlatform.PLATFORM_CAKERESUME,
            CategorySource.parent_source_id is not None
        ).all()
        updated_categories = [{ "name": r[0], "parent_id": r[1] } for r in results]
except Exception as e:
    print(f"Error querying database: {e}")

print(json.dumps(updated_categories, ensure_ascii=False, indent=4))
