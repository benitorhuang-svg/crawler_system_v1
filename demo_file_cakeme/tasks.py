# crawler/projects/platform_cakeresume/tasks.py
"""
Celery task for Cakeresume. Extracts categories by parsing the
__NEXT_DATA__ script tag from the main jobs page HTML.
This method is fast, reliable, and avoids browser automation.
"""
import logging
import json
from typing import List, Dict, Any

from bs4 import BeautifulSoup

from crawler.app import app
from crawler.enums import SourcePlatform
from crawler.database import repository
from crawler.utils import make_request
from crawler.settings import settings

logger = logging.getLogger(__name__)


def parse_next_data_for_i18n_categories(html_content: str) -> List[Dict[str, Any]]:
    """
    Finds the __NEXT_DATA__ script tag, parses its JSON content,
    and extracts the hierarchical category data from the i18n (internationalization) object.
    This is the most reliable method.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    next_data_script = soup.find('script', id='__NEXT_DATA__')
    
    if not next_data_script or not hasattr(next_data_script, 'string') or not next_data_script.string:
        raise ValueError("Could not find __NEXT_DATA__ script tag or it is empty in the HTML.")

    try:
        data = json.loads(next_data_script.string)
    except json.JSONDecodeError:
        raise ValueError("Failed to parse JSON from __NEXT_DATA__ script tag.")

    try:
        # The definitive path to the translation data
        i18n_data = data['props']['pageProps']['_nextI18Next']['initialI18nStore']['zh-TW']['profession']
    except KeyError as e:
        raise ValueError(f"Unexpected JSON structure in __NEXT_DATA__. Missing key: {e}")

    flat_list = []
    parent_map = {}

    # First pass: Get all parent categories (e.g., "profession_groups.it": "軟體")
    for key, value in i18n_data.items():
        if key.startswith("profession_groups."):
            parent_id = key.replace("profession_groups.", "")
            parent_name = value
            parent_map[parent_id] = parent_name
            flat_list.append({
                "source_platform": SourcePlatform.PLATFORM_CAKERESUME,
                "source_category_id": parent_id,
                "source_category_name": parent_name,
                "parent_source_id": None,
            })

    # Second pass: Get all sub-categories and link them to parents
    for key, value in i18n_data.items():
        if key.startswith("professions."):
            full_id = key.replace("professions.", "")
            parts = full_id.split('_', 1)
            if len(parts) > 1:
                parent_id = parts[0]
                if parent_id in parent_map:
                    flat_list.append({
                        "source_platform": SourcePlatform.PLATFORM_CAKERESUME,
                        "source_category_id": full_id,
                        "source_category_name": value,
                        "parent_source_id": parent_id,
                    })
                else:
                    logger.warning(f"Found orphan sub-category '{full_id}' with no matching parent '{parent_id}'. Skipping.")

    logger.info(f"Successfully extracted {len(flat_list)} categories from __NEXT_DATA__.")
    return flat_list


@app.task(bind=True, name="platform_cakeresume.run_category_pipeline", acks_late=True, time_limit=300, queue='category_queue')
def run_category_pipeline(self) -> None:
    """
    Fetches Cakeresume job categories by parsing the initial
    server-side rendered page data from the __NEXT_DATA__ tag.
    """
    try:
        logger.info("[Cakeresume] Running category pipeline from __NEXT_DATA__ (Final Version)...")
        cfg = settings.pcake
        
        res = make_request("https://www.cakeresume.com/jobs", headers=cfg.headers)
        
        categories = parse_next_data_for_i18n_categories(res.text)
        
        if not categories:
            logger.warning("[Cakeresume] No categories were extracted from __NEXT_DATA__. Aborting sync.")
            return

        result = repository.sync_source_categories(SourcePlatform.PLATFORM_CAKERESUME, categories)
        logger.info(f"[Cakeresume] Category pipeline finished successfully. Sync result: {result}")

    except Exception as e:
        logger.error(f"[Cakeresume] Category pipeline failed: {e}", exc_info=True)
        raise self.retry(exc=e, countdown=180)