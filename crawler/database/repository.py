import structlog
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.dialects.mysql import insert

from crawler.database.connection import get_session

from crawler.database.models import (
    CategorySource,
    Url,
    Job,
    SourcePlatform,
    JobStatus,
    CrawlStatus,
    JobPydantic,
    CategorySourcePydantic,
)

logger = structlog.get_logger(__name__)


def sync_source_categories(
    platform: SourcePlatform, flattened_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    將抓取到的職務分類數據同步到資料庫。
    執行 UPSERT 操作，如果分類已存在則更新，否則插入。
    """
    if not flattened_data:
        logger.info("No flattened data to sync for categories.", platform=platform.value)
        return {"total": 0, "affected": 0}

    with get_session() as session:
        stmt = insert(CategorySource).values(flattened_data)
        update_dict = {
            "source_category_name": stmt.inserted.source_category_name,
            "parent_source_id": stmt.inserted.parent_source_id,
        }
        stmt = stmt.on_duplicate_key_update(**update_dict)
        session.execute(stmt)
        logger.info(
            "Categories synced successfully.",
            platform=platform.value,
            total_categories=len(flattened_data),
        )
        return {"total": len(flattened_data), "affected": 0}


def get_source_categories(
    platform: SourcePlatform, source_ids: Optional[List[str]] = None
) -> List[CategorySourcePydantic]:
    """
    從資料庫獲取指定平台和可選的 source_ids 的職務分類。
    返回 CategorySourcePydantic 實例列表，以便在 Session 關閉後安全使用。
    """
    with get_session() as session:
        stmt = select(CategorySource).where(CategorySource.source_platform == platform)
        if source_ids:
            stmt = stmt.where(CategorySource.source_category_id.in_(source_ids))
        
        categories = [
            CategorySourcePydantic.model_validate(cat)
            for cat in session.scalars(stmt).all()
        ]
        logger.debug("Fetched source categories.", platform=platform.value, count=len(categories), source_ids=source_ids)
        return categories

def get_all_categories_for_platform(platform: SourcePlatform) -> List[CategorySourcePydantic]:
    """
    從資料庫獲取指定平台的所有職務分類。
    返回 CategorySourcePydantic 實例列表。
    """
    with get_session() as session:
        stmt = select(CategorySource).where(CategorySource.source_platform == platform)
        categories = [
            CategorySourcePydantic.model_validate(cat)
            for cat in session.scalars(stmt).all()
        ]
        logger.debug("Fetched all categories for platform.", platform=platform.value, count=len(categories))
        return categories

def upsert_urls(platform: SourcePlatform, urls: List[str]) -> None:
    """
    Synchronizes a list of URLs for a given platform with the database.
    Performs an UPSERT operation. URLs are marked as ACTIVE and PENDING.
    """
    if not urls:
        logger.info("No URLs to upsert.", platform=platform.value)
        return

    now = datetime.now(timezone.utc)
    url_models_to_upsert = [
        {
            "source_url": url,
            "source": platform,
            "status": JobStatus.ACTIVE,
            "details_crawl_status": CrawlStatus.PENDING.value, # 使用大寫
            "crawled_at": now,
            "updated_at": now,
        }
        for url in urls
    ]

    with get_session() as session:
        stmt = insert(Url).values(url_models_to_upsert)
        update_dict = {
            "status": stmt.inserted.status,
            "updated_at": stmt.inserted.updated_at,
            "details_crawl_status": stmt.inserted.details_crawl_status,
        }
        stmt = stmt.on_duplicate_key_update(**update_dict)
        session.execute(stmt)
        logger.info("URLs upserted successfully.", platform=platform.value, count=len(urls))


def get_urls_by_crawl_status(
    platform: SourcePlatform, statuses: List[CrawlStatus], limit: int
) -> List[str]:
    """
    從資料庫獲取指定平台和指定爬取狀態列表的 URL。
    返回 URL 字串列表。
    """
    with get_session() as session:
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


def update_urls_status(urls: List[str], status: CrawlStatus) -> None:
    """
    批量更新一組 URL 的爬取狀態。
    """
    if not urls:
        logger.info("No URLs provided to update status.")
        return

    now = datetime.now(timezone.utc)
    with get_session() as session:
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


def upsert_jobs(jobs: List[JobPydantic]) -> None:
    """
    將 Job 對象列表同步到資料庫。
    執行 UPSERT 操作，如果職位已存在則更新，否則插入。
    """
    if not jobs:
        logger.info("No jobs to upsert.", count=0)
        return

    now = datetime.now(timezone.utc)
    job_dicts_to_upsert = [
        {
            **job.model_dump(exclude_none=False),
            "updated_at": now,
            "created_at": job.created_at or now,
        }
        for job in jobs
    ]

    with get_session() as session:
        stmt = insert(Job).values(job_dicts_to_upsert)

        update_cols = {
            column.name: getattr(stmt.inserted, column.name)
            for column in Job.__table__.columns
            if not column.primary_key
        }

        final_stmt = stmt.on_duplicate_key_update(**update_cols)
        session.execute(final_stmt)
        logger.info("Jobs upserted successfully.", count=len(job_dicts_to_upsert))


def mark_urls_as_crawled(processed_urls: Dict[CrawlStatus, List[str]]) -> None:
    """
    根據處理狀態標記 URL 為已爬取。
    """
    if not processed_urls:
        logger.info("No URLs to mark as crawled.")
        return

    now = datetime.now(timezone.utc)
    with get_session() as session:
        for status, urls in processed_urls.items():
            if urls:
                stmt = (
                    update(Url)
                    .where(Url.source_url.in_(urls))
                    .values(details_crawl_status=status, details_crawled_at=now)
                )
                session.execute(stmt)
                logger.info("URLs marked as crawled.", status=status.value, count=len(urls))