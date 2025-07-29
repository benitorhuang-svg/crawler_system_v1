import structlog
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from sqlalchemy.orm import Session
from sqlalchemy import select, update
from sqlalchemy.dialects.mysql import insert

from crawler.database.connection import get_engine
from crawler.database.models import (
    CategorySource,
    Url,
    Job,
    SourcePlatform,
    JobStatus,
    CrawlStatus,
    JobPydantic,
)

# Configure logging
logger = structlog.get_logger(__name__)


def sync_source_categories(
    platform: SourcePlatform, flattened_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    將抓取到的職務分類數據同步到資料庫。
    執行 UPSERT 操作，如果分類已存在則更新，否則插入。
    """
    if not flattened_data:
        return {"total": 0, "affected": 0}

    with Session(get_engine()) as session:
        stmt = insert(CategorySource).values(flattened_data)
        update_dict = {
            "source_category_name": stmt.inserted.source_category_name,
            "parent_source_id": stmt.inserted.parent_source_id,
        }
        stmt = stmt.on_duplicate_key_update(**update_dict)
        session.execute(stmt)
        session.commit()
        logger.info(
            "Synced categories",
            platform=platform.value,
            total_categories=len(flattened_data),
        )
        return {"total": len(flattened_data), "affected": 0}


def get_source_categories(
    platform: SourcePlatform, source_ids: Optional[List[str]] = None
) -> List[CategorySource]:
    """
    從資料庫獲取指定平台和可選的 source_ids 的職務分類。
    """
    with Session(get_engine()) as session:
        stmt = select(CategorySource).where(CategorySource.source_platform == platform)
        if source_ids:
            stmt = stmt.where(CategorySource.source_category_id.in_(source_ids))
        return list(session.scalars(stmt).all())

def upsert_urls(platform: SourcePlatform, urls: List[str]) -> None:
    """
    Synchronizes a list of URLs for a given platform with the database.
    Performs an UPSERT operation. URLs are marked as ACTIVE and PENDING.
    """
    if not urls:
        return

    now = datetime.now(timezone.utc)
    url_models_to_upsert = [
        {
            "source_url": url,
            "source": platform,
            "status": JobStatus.ACTIVE,
            "details_crawl_status": CrawlStatus.PENDING,
            "crawled_at": now,
            "updated_at": now,
        }
        for url in urls
    ]

    with Session(get_engine()) as session:
        stmt = insert(Url).values(url_models_to_upsert)
        update_dict = {
            "status": stmt.inserted.status,
            "updated_at": stmt.inserted.updated_at,
            "details_crawl_status": stmt.inserted.details_crawl_status,
        }
        stmt = stmt.on_duplicate_key_update(**update_dict)
        session.execute(stmt)
        session.commit()


def get_unprocessed_urls(platform: SourcePlatform, limit: int) -> List[Url]:
    """
    從資料庫獲取指定平台未處理的 URL 列表。
    """
    with Session(get_engine()) as session:
        statement = (
            select(Url)
            .where(Url.source == platform, Url.details_crawl_status == CrawlStatus.PENDING)
            .limit(limit)
        )
        return list(session.scalars(statement).all())


def upsert_jobs(jobs: List[JobPydantic]) -> None:
    """
    將 Job 對象列表同步到資料庫。
    執行 UPSERT 操作，如果職位已存在則更新，否則插入。
    """
    if not jobs:
        return

    now = datetime.now(timezone.utc)
    job_dicts_to_upsert = [
        {
            **dump_job,
            "updated_at": now,
            "created_at": dump_job.get("created_at") or now,
        }
        for job in jobs
        for dump_job in [job.model_dump(exclude_none=False)]
    ]

    with Session(get_engine()) as session:
        try:
            stmt = insert(Job).values(job_dicts_to_upsert)

            update_cols = {
                column.name: getattr(stmt.inserted, column.name)
                for column in Job.__table__.columns
                if not column.primary_key
            }

            final_stmt = stmt.on_duplicate_key_update(**update_cols)
            session.execute(final_stmt)
            session.commit()
            logger.info("Upserted or updated jobs", count=len(job_dicts_to_upsert))

        except Exception as e:
            session.rollback()
            logger.error("Failed to upsert jobs", error=e, exc_info=True)
            raise


def mark_urls_as_crawled(processed_urls: Dict[CrawlStatus, List[str]]) -> None:
    """
    根據處理狀態標記 URL 為已爬取。
    """
    now = datetime.now(timezone.utc)
    with Session(get_engine()) as session:
        for status, urls in processed_urls.items():
            if urls:
                stmt = (
                    update(Url)
                    .where(Url.source_url.in_(urls))
                    .values(details_crawl_status=status, details_crawled_at=now)
                )
                session.execute(stmt)
        session.commit()
