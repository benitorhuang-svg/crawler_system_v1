import structlog
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import DeclarativeBase

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


def _generic_upsert(
    model: DeclarativeBase, data_list: List[Dict[str, Any]], update_columns: List[str]
) -> None:
    """
    通用的 UPSERT 函式，用於將數據同步到資料庫。
    如果記錄已存在，則更新指定欄位；否則插入新記錄。
    """
    if not data_list:
        return

    with get_session() as session:
        stmt = insert(model).values(data_list)
        update_dict = {col: getattr(stmt.inserted, col) for col in update_columns}
        stmt = stmt.on_duplicate_key_update(**update_dict)
        result = session.execute(stmt)
        return result.rowcount # 返回受影響的行數


def sync_source_categories(
    platform: SourcePlatform, flattened_data: List[Dict[str, Any]]
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
    affected_rows = _generic_upsert(CategorySource, flattened_data, update_cols)

    logger.info(
        "Categories synced successfully.",
        platform=platform.value,
        total_categories=len(flattened_data),
        affected_rows=affected_rows,
    )
    return {"total": len(flattened_data), "affected": affected_rows}


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
        logger.debug(
            "Fetched source categories.",
            platform=platform.value,
            count=len(categories),
            source_ids=source_ids,
        )
        return categories


def get_all_categories_for_platform(
    platform: SourcePlatform,
) -> List[CategorySourcePydantic]:
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
        logger.debug(
            "Fetched all categories for platform.",
            platform=platform.value,
            count=len(categories),
        )
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
            "status": JobStatus.ACTIVE.value,
            "details_crawl_status": CrawlStatus.PENDING.value,
            "crawled_at": now,
            "updated_at": now,
        }
        for url in urls
    ]

    update_cols = ["status", "updated_at", "details_crawl_status"]
    affected_rows = _generic_upsert(Url, url_models_to_upsert, update_cols)

    logger.info("URLs upserted successfully.", platform=platform.value, count=len(urls), affected_rows=affected_rows)


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

    # 動態生成更新欄位列表，排除主鍵
    update_cols = [
        column.name for column in Job.__table__.columns if not column.primary_key
    ]
    affected_rows = _generic_upsert(Job, job_dicts_to_upsert, update_cols)

    logger.info("Jobs upserted successfully.", count=len(job_dicts_to_upsert), affected_rows=affected_rows)


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
                logger.info(
                    "URLs marked as crawled.", status=status.value, count=len(urls)
                )
