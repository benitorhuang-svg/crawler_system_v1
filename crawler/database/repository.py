import structlog
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timezone, timedelta
import pandas as pd

from sqlalchemy import select, update, delete
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import DeclarativeBase

from crawler.database.connection import get_session

from crawler.database.models import (
    CategorySource,
    Url,
    UrlCategory,
    Job104,
    Job1111,
    JobCakeresume,
    JobYes123,
    JobYourator,
    JobLocation104,
    JobLocation1111,
    JobLocationCakeresume,
    JobLocationYes123,
    JobLocationYourator,
)
from crawler.database.schemas import (
    SourcePlatform,
    JobStatus,
    CrawlStatus,
    JobPydantic,
    CategorySourcePydantic,
    JobLocationPydantic,
)

logger = structlog.get_logger(__name__)


def _generic_upsert(
    model: DeclarativeBase, data_list: List[Dict[str, Any]], update_columns: List[str], db_name: str = None
) -> int:
    """
    通用的 UPSERT 函式，用於將數據同步到資料庫。
    如果記錄已存在則更新指定欄位；否則插入新記錄。
    """
    if not data_list:
        return 0

    with get_session(db_name=db_name) as session:
        stmt = insert(model).values(data_list)
        
        if update_columns: # 只有當有需要更新的欄位時才構建 update_dict
            update_dict = {col: getattr(stmt.inserted, col) for col in update_columns}
            stmt = stmt.on_duplicate_key_update(**update_dict)
        else:
            # 對於沒有額外更新欄位的 UPSERT (例如只有複合主鍵的關聯表)，
            # 仍然需要呼叫 on_duplicate_key_update 以觸發 ON DUPLICATE KEY 行為
            # 這裡選擇更新第一個非主鍵欄位為其自身，以滿足語法要求但不實際更新數據
            # 如果沒有非主鍵欄位，則選擇第一個主鍵欄位
            first_column_name = next(iter(model.__table__.columns)).name
            stmt = stmt.on_duplicate_key_update(**{first_column_name: getattr(stmt.inserted, first_column_name)})

        result = session.execute(stmt)
        return result.rowcount # 返回受影響的行數


def clear_urls_and_categories(db_name: str = None) -> None:
    """
    清空 tb_urls 和 tb_url_categories 資料表。
    """
    with get_session(db_name=db_name) as session:
        session.execute(delete(UrlCategory))
        session.execute(delete(Url))
        session.commit()
        logger.info("已清空 tb_urls 和 tb_url_categories 資料表。")


def sync_source_categories(
    platform: SourcePlatform, flattened_data: List[Dict[str, Any]], db_name: str = None
) -> Dict[str, Any]:
    """
    將抓取到的職務分類數據同步到資料庫。
    執行 UPSERT 操作，如果分類已存在則更新，否則插入。
    """
    if not flattened_data:
        logger.info(
            "No flattened data to sync for categories.", platform=platform.value
        )
        return {"total": 0, "affected": 0}

    update_cols = ["source_category_name", "parent_source_id"]
    affected_rows = _generic_upsert(CategorySource, flattened_data, update_cols, db_name=db_name)

    logger.info(
        "Categories synced successfully.",
        platform=platform.value,
        total_categories=len(flattened_data),
        affected_rows=affected_rows,
    )
    return {"total": len(flattened_data), "affected": affected_rows}


def get_source_categories(
    platform: SourcePlatform, source_ids: Optional[List[str]] = None, db_name: str = None
) -> List[CategorySourcePydantic]:
    """
    從資料庫獲取指定平台和可選的 source_ids 的職務分類。
    返回 CategorySourcePydantic 實例列表，以便在 Session 關閉後安全使用。
    """
    with get_session(db_name=db_name) as session:
        stmt = select(CategorySource).where(CategorySource.source_platform == platform)
        if source_ids:
            stmt = stmt.where(CategorySource.source_category_id.in_(source_ids))

        categories = [
            CategorySourcePydantic.model_validate(cat)
            for cat in session.scalars(stmt).all()
        ]
        logger.debug(
            "Fetched source categories.",
            platform=platform.value,
            count=len(categories),
            source_ids=source_ids,
        )
        return categories


def get_all_categories_for_platform(
    platform: SourcePlatform, db_name: str = None
) -> List[CategorySourcePydantic]:
    """
    從資料庫獲取指定平台的所有職務分類。
    返回 CategorySourcePydantic 實例列表。
    """
    with get_session(db_name=db_name) as session:
        stmt = select(CategorySource).where(CategorySource.source_platform == platform)
        categories = [
            CategorySourcePydantic.model_validate(cat)
            for cat in session.scalars(stmt).all()
        ]
        logger.debug(
            "Fetched all categories for platform.",
            platform=platform.value,
            count=len(categories),
        )
        return categories


def upsert_urls(platform: SourcePlatform, urls: List[str], db_name: str = None) -> None:
    """
    Synchronizes a list of URLs for a given platform with the database。
    Performs an UPSERT operation. URLs are marked as ACTIVE and PENDING.
    """
    if not urls:
        logger.info("No URLs to upsert.", platform=platform.value)
        return

    logger.info("Attempting to upsert URLs.", platform=platform.value, count=len(urls), db=db_name)

    now = datetime.now(timezone.utc)
    url_models_to_upsert = [
        {
            "source_url": url,
            "source": platform,
            "status": JobStatus.ACTIVE.value,
            "details_crawl_status": CrawlStatus.QUEUED.value,
            "crawled_at": now,
            "updated_at": now,
        }
        for url in urls
    ]

    update_cols = ["status", "updated_at", "details_crawl_status"]
    affected_rows = _generic_upsert(Url, url_models_to_upsert, update_cols, db_name=db_name)

    logger.info("URLs upserted successfully.", platform=platform.value, count=len(urls), affected_rows=affected_rows)


def get_urls_by_crawl_status(
    platform: SourcePlatform, statuses: List[CrawlStatus], limit: int, db_name: str = None
) -> List[str]:
    """
    從資料庫獲取指定平台和指定爬取狀態列表的 URL。
    返回 URL 字串列表。
    """
    with get_session(db_name=db_name) as session:
        statement = (
            select(Url.source_url)
            .where(Url.source == platform, Url.details_crawl_status.in_(statuses))
            .limit(limit)
        )
        urls = list(session.scalars(statement).all())
        logger.debug(
            "Fetched URLs by crawl status.",
            platform=platform.value,
            statuses=[s.value for s in statuses],
            count=len(urls),
            limit=limit,
        )
        return urls


def update_urls_status(urls: List[str], status: CrawlStatus, db_name: str = None) -> None:
    """
    批量更新一組 URL 的爬取狀態。
    """
    if not urls:
        logger.info("No URLs provided to update status.")
        return

    now = datetime.now(timezone.utc)
    with get_session(db_name=db_name) as session:
        stmt = (
            update(Url)
            .where(Url.source_url.in_(urls))
            .values(details_crawl_status=status, updated_at=now)
        )
        session.execute(stmt)
        logger.info(
            "Successfully updated URL statuses.",
            status=status.value,
            count=len(urls),
        )


def upsert_jobs(jobs: List[JobPydantic], db_name: str = None) -> None:
    """
    將 Job 對象列表同步到資料庫。
    執行 UPSERT 操作，如果職位已存在則更新，否則插入。
    """
    if not jobs:
        logger.info("No jobs to upsert.", count=0)
        return

    # Map SourcePlatform to the corresponding Job model
    platform_job_map = {
        SourcePlatform.PLATFORM_104: Job104,
        SourcePlatform.PLATFORM_1111: Job1111,
        SourcePlatform.PLATFORM_CAKERESUME: JobCakeresume,
        SourcePlatform.PLATFORM_YES123: JobYes123,
        SourcePlatform.PLATFORM_YOURATOR: JobYourator,
    }

    # Group jobs by platform
    jobs_by_platform: Dict[SourcePlatform, List[Dict[str, Any]]] = {}
    for job in jobs:
        platform = job.source_platform
        if platform not in jobs_by_platform:
            jobs_by_platform[platform] = []
        jobs_by_platform[platform].append(
            {
                **job.model_dump(
                    exclude_none=False,
                    exclude={"extracted_skills", "extracted_languages", "extracted_licenses"}
                ),
                "updated_at": datetime.now(timezone.utc),
                "created_at": job.created_at or datetime.now(timezone.utc),
            }
        )

    total_affected_rows = 0
    for platform, job_dicts_to_upsert in jobs_by_platform.items():
        job_model = platform_job_map.get(platform)
        if not job_model:
            logger.warning(f"No Job model found for platform: {platform.value}. Skipping upsert for these jobs.")
            continue

        # 動態生成更新欄位列表，排除主鍵
        update_cols = [
            column.name for column in job_model.__table__.columns if not column.primary_key
        ]
        affected_rows = _generic_upsert(job_model, job_dicts_to_upsert, update_cols, db_name=db_name)
        total_affected_rows += affected_rows
        logger.info(f"Jobs upserted successfully for platform {platform.value}.", count=len(job_dicts_to_upsert), affected_rows=affected_rows)

    logger.info("Total jobs upserted successfully.", count=len(jobs), affected_rows=total_affected_rows)


def upsert_job_locations(job_locations: List[JobLocationPydantic], db_name: str = None) -> None:
    """
    將 JobLocation 對象列表同步到資料庫。
    執行 UPSERT 操作，如果經緯度資訊已存在則更新，否則插入。
    """
    if not job_locations:
        logger.info("No job locations to upsert.", count=0)
        return

    # Map SourcePlatform to the corresponding JobLocation model
    platform_job_location_map = {
        SourcePlatform.PLATFORM_104: JobLocation104,
        SourcePlatform.PLATFORM_1111: JobLocation1111,
        SourcePlatform.PLATFORM_CAKERESUME: JobLocationCakeresume,
        SourcePlatform.PLATFORM_YES123: JobLocationYes123,
        SourcePlatform.PLATFORM_YOURATOR: JobLocationYourator,
    }

    # Group job locations by platform
    job_locations_by_platform: Dict[SourcePlatform, List[Dict[str, Any]]] = {}
    for loc in job_locations:
        platform = loc.source_platform
        if platform not in job_locations_by_platform:
            job_locations_by_platform[platform] = []
        job_locations_by_platform[platform].append(
            {
                **loc.model_dump(exclude_none=False),
                "updated_at": datetime.now(timezone.utc),
                "created_at": loc.created_at or datetime.now(timezone.utc),
            }
        )

    total_affected_rows = 0
    for platform, location_dicts_to_upsert in job_locations_by_platform.items():
        job_location_model = platform_job_location_map.get(platform)
        if not job_location_model:
            logger.warning(f"No JobLocation model found for platform: {platform.value}. Skipping upsert for these job locations.")
            continue

        update_cols = ["latitude", "longitude", "updated_at"]
        affected_rows = _generic_upsert(job_location_model, location_dicts_to_upsert, update_cols, db_name=db_name)
        total_affected_rows += affected_rows
        logger.info(f"Job locations upserted successfully for platform {platform.value}.", count=len(location_dicts_to_upsert), affected_rows=affected_rows)

    logger.info("Total job locations upserted successfully.", count=len(job_locations), affected_rows=total_affected_rows)


def upsert_url_categories(url_category_data: List[Dict[str, Any]], db_name: str = None) -> None:
    """
    將 URL 與其所屬的分類關聯數據同步到資料庫。
    執行 UPSERT 操作，如果關聯已存在則更新，否則插入。
    """
    if not url_category_data:
        logger.info("No URL category data to upsert.")
        return

    logger.info("Attempting to upsert URL categories.", count=len(url_category_data), db=db_name)

    affected_rows = _generic_upsert(UrlCategory, url_category_data, [], db_name=db_name)

    logger.info("URL categories upserted successfully.", count=len(url_category_data), affected_rows=affected_rows)


def mark_urls_as_crawled(processed_urls: Dict[CrawlStatus, List[str]], db_name: str = None) -> None:
    """
    根據處理狀態標記 URL 為已爬取。
    """
    if not processed_urls:
        logger.info("No URLs to mark as crawled.")
        return

    now = datetime.now(timezone.utc)
    with get_session(db_name=db_name) as session:
        for status, urls in processed_urls.items():
            if urls:
                stmt = (
                    update(Url)
                    .where(Url.source_url.in_(urls))
                    .values(details_crawl_status=status, details_crawled_at=now)
                )
                session.execute(stmt)
                logger.info(
                    "URLs marked as crawled.", status=status.value, count=len(urls)
                )

def update_category_parent_id(
    platform: SourcePlatform, source_category_id: str, new_parent_source_id: Optional[str], db_name: str = None
) -> None:
    """
    更新指定平台和 source_category_id 的職務分類的 parent_source_id。
    """
    with get_session(db_name=db_name) as session:
        stmt = (
            update(CategorySource)
            .where(
                CategorySource.source_platform == platform,
                CategorySource.source_category_id == source_category_id,
            )
            .values(parent_source_id=new_parent_source_id)
        )
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info(
                "Updated parent_source_id for category.",
                platform=platform.value,
                source_category_id=source_category_id,
                new_parent_source_id=new_parent_source_id,
            )
        else:
            logger.warning(
                "Category not found for update.",
                platform=platform.value,
                source_category_id=source_category_id,
            )

def get_all_category_source_ids_pandas(platform: SourcePlatform, db_name: str = None) -> Set[str]:
    """
    使用 Pandas 獲取指定平台所有職務分類的 source_category_id。
    """
    with get_session(db_name=db_name) as session:
        query = select(CategorySource.source_category_id).where(CategorySource.source_platform == platform)
        df = pd.read_sql(query, session.bind)
        return set(df["source_category_id"].tolist())


def get_all_crawled_category_ids_pandas(platform: SourcePlatform, db_name: str = None) -> Set[str]:
    """
    使用 Pandas 獲取指定平台所有已爬取 URL 的 source_category_id。
    """
    with get_session(db_name=db_name) as session:
        query = select(UrlCategory.source_category_id).join(Url, UrlCategory.source_url == Url.source_url).where(Url.source == platform)
        df = pd.read_sql(query, session.bind)
        return set(df["source_category_id"].tolist())


def get_root_categories(platform: SourcePlatform, db_name: str = None) -> List[CategorySourcePydantic]:
    """
    從資料庫獲取指定平台所有 parent_source_id 為 NULL 的職務分類 (即根分類)。
    """
    with get_session(db_name=db_name) as session:
        stmt = select(CategorySource).where(
            CategorySource.source_platform == platform,
            CategorySource.parent_source_id.is_(None)  # Filter for NULL parent_source_id
        )
        categories = [
            CategorySourcePydantic.model_validate(cat)
            for cat in session.scalars(stmt).all()
        ]
        logger.debug(
            "Fetched root categories for platform.",
            platform=platform.value,
            count=len(categories),
        )
        return categories


def get_stale_crawled_category_ids_pandas(platform: SourcePlatform, n_days: int, db_name: str = None) -> Set[str]:
    """
    使用 Pandas 獲取指定平台中，上次爬取時間超過 n_days 的 source_category_id。
    """
    threshold_date = datetime.now(timezone.utc) - timedelta(days=n_days)
    with get_session(db_name=db_name) as session:
        query = (
            select(UrlCategory.source_category_id)
            .join(Url, UrlCategory.source_url == Url.source_url)
            .where(
                Url.source == platform,
                UrlCategory.created_at < threshold_date
            )
            .distinct()
        )
        df = pd.read_sql(query, session.bind)
        return set(df["source_category_id"].tolist())