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
    Job,
    Company,
    Location,
    Skill,
    JobLocation,
    JobSkill,
    JobCategoryTag,
    JobObservation,
)
from crawler.database.schemas import (
    SourcePlatform,
    JobStatus,
    CrawlStatus,
    JobPydantic,
    UrlPydantic,
    CategorySourcePydantic,
    CompanyPydantic,
    LocationPydantic,
    SkillPydantic,
    JobObservationPydantic,
)

logger = structlog.get_logger(__name__)


def insert_job_observations(job_observations: List[JobObservationPydantic], db_name: str = None) -> None:
    """
    將職缺觀察記錄插入到 tb_job_observations 表格中。
    """
    if not job_observations:
        logger.info("No job observations to insert.", count=0)
        return

    with get_session(db_name=db_name) as session:
        data_to_insert = [obs.model_dump() for obs in job_observations]
        session.bulk_insert_mappings(JobObservation, data_to_insert)
        session.commit()
        logger.info(f"Successfully inserted {len(job_observations)} job observations.")


def _generic_upsert(
    session,
    model: DeclarativeBase, 
    data_list: List[Dict[str, Any]], 
    update_columns: List[str],
    # index_elements: List[str] # This parameter is not directly used by on_duplicate_key_update
) -> int:
    """
    通用的 UPSERT 函式，返回插入或更新的記錄的影響行數。
    """
    if not data_list:
        return 0

    stmt = insert(model).values(data_list)
    
    update_dict = {col: getattr(stmt.inserted, col) for col in update_columns}
    
    # on_duplicate_key_update automatically uses primary key or unique constraints for conflict detection
    upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
    
    result = session.execute(upsert_stmt)
    
    return result.rowcount


def upsert_companies(session, companies: List[CompanyPydantic]) -> Dict[str, str]:
    if not companies:
        return {}

    company_map = {}
    for company in companies:
        # Use source_company_id and source_platform as primary key for querying
        existing_company = session.query(Company).filter_by(
            source_platform=company.source_platform,
            source_company_id=company.source_company_id
        ).first()

        if existing_company:
            existing_company.name = company.name
            existing_company.url = company.url
            
            existing_company.updated_at = datetime.now(timezone.utc)
            session.add(existing_company)
            company_map[company.source_company_id] = existing_company.source_company_id # Map to source_company_id
        else:
            new_company = Company(**company.model_dump())
            session.add(new_company)
            session.flush() # Flush to get the ID if it's auto-generated, though here it's source_company_id
            company_map[company.source_company_id] = new_company.source_company_id # Map to source_company_id
            
    return company_map

def upsert_locations(session, locations: List[LocationPydantic]) -> Dict[str, int]:
    if not locations:
        return {}

    location_map = {}
    for loc in locations:
        addr = loc.address_detail
        existing_location = session.query(Location).filter_by(address_detail=addr).first()

        if existing_location:
            location_map[addr] = existing_location.id
            # If the existing address is different from the cleaned one, update it
            if existing_location.address_detail != addr:
                existing_location.address_detail = addr
                session.add(existing_location)
        else:
            new_location = Location(**loc.model_dump())
            session.add(new_location)
            session.flush() # Flush to get the ID
            location_map[addr] = new_location.id
            
    return location_map

def upsert_skills(session, skills: List[SkillPydantic]) -> Dict[str, str]:
    if not skills:
        return {}

    skill_map = {}
    for skill in skills:
        skill_name = skill.name
        # Use name as primary key for querying
        existing_skill = session.query(Skill).filter_by(name=skill_name).first()

        if existing_skill:
            skill_map[skill_name] = existing_skill.name # Map to name
        else:
            new_skill = Skill(name=skill_name) # name is the primary key
            session.add(new_skill)
            session.flush()
            skill_map[skill_name] = new_skill.name # Map to name
            
    return skill_map

def upsert_job_location_association(
    session, job_id: str, location_id: int
) -> None:
    """
    Upserts an association between a job and a location in tb_job_locations.
    """
    # Check if the association already exists
    existing_association = session.query(JobLocation).filter_by(
        job_id=job_id, location_id=location_id
    ).first()

    if not existing_association:
        new_association = JobLocation(job_id=job_id, location_id=location_id)
        session.add(new_association)
        logger.debug(
            "Added new job-location association.",
            job_id=job_id,
            location_id=location_id
        )
    else:
        logger.debug(
            "Job-location association already exists.",
            job_id=job_id,
            location_id=location_id
        )

def upsert_jobs(jobs: List[JobPydantic], db_name: str = None) -> None:
    if not jobs:
        logger.info("No jobs to upsert.", count=0)
        return

    with get_session(db_name=db_name) as session:
        all_companies = [j.company for j in jobs if j.company]
        all_locations = [loc for j in jobs for loc in j.locations]
        all_skills = [skill for j in jobs for skill in j.skills]

        company_id_map = upsert_companies(session, all_companies)
        location_id_map = upsert_locations(session, all_locations)
        skill_id_map = upsert_skills(session, all_skills)

        for job in jobs:
            # Use source_company_id as company_id
            company_id = company_id_map.get(job.company.source_company_id) if job.company else None
            
            # Exclude primary key from model_dump for upsert, as it's handled by insert().values()
            job_data = job.model_dump(exclude={'company', 'locations', 'skills', 'category_tags', 'source_job_id'})
            job_data['company_id'] = company_id
            job_data['source_job_id'] = job.source_job_id # Add source_job_id back for insert/update

            # Use source_job_id as primary key for querying
            existing_job = session.query(Job).filter_by(
                source_platform=job.source_platform, 
                source_job_id=job.source_job_id
            ).first()

            if existing_job:
                # Check if the incoming job data is newer based on posted_at
                should_update_job_data = True
                if job.posted_at and existing_job.posted_at:
                    incoming_posted_at_utc = job.posted_at.astimezone(timezone.utc) if job.posted_at.tzinfo else job.posted_at.replace(tzinfo=timezone.utc)
                    existing_posted_at_utc = existing_job.posted_at.astimezone(timezone.utc) if existing_job.posted_at.tzinfo else existing_job.posted_at.replace(tzinfo=timezone.utc)

                    if incoming_posted_at_utc < existing_posted_at_utc:
                        logger.debug("Skipping update for job data as incoming posted_at is older.",
                                     job_id=job.source_job_id,
                                     incoming_posted_at=job.posted_at,
                                     existing_posted_at=existing_job.posted_at)
                        should_update_job_data = False
                
                if should_update_job_data:
                    for key, value in job_data.items():
                        setattr(existing_job, key, value)
                    session.add(existing_job)
                else:
                    # Even if job data is not updated, ensure updated_at is refreshed to reflect observation
                    existing_job.updated_at = datetime.now(timezone.utc)
                    session.add(existing_job)
                # job.id = existing_job.source_job_id # No longer 'id', but source_job_id
            else:
                new_job = Job(**job_data) # source_job_id is part of job_data
                session.add(new_job)
                session.flush()
                # job.id = new_job.source_job_id # No longer 'id', but source_job_id

            # Handle JobLocation associations
            if job.locations:
                # Delete existing associations for this job
                session.query(JobLocation).filter_by(job_id=job.source_job_id).delete()
                
                added_location_ids_for_job = set() # Track unique location_ids for this job
                for loc in job.locations:
                    location_id = location_id_map.get(loc.address_detail)
                    if location_id and location_id not in added_location_ids_for_job:
                        session.add(JobLocation(job_id=job.source_job_id, location_id=location_id))
                        added_location_ids_for_job.add(location_id)
            
            # Handle JobSkill associations
            if job.skills:
                # Delete existing associations for this job
                session.query(JobSkill).filter_by(job_id=job.source_job_id).delete()
                for skill in job.skills:
                    skill_name = skill_id_map.get(skill.name) # Get the actual skill name (which is the PK)
                    if skill_name:
                        session.add(JobSkill(job_id=job.source_job_id, skill_id=skill_name))

            # Handle JobCategoryTag associations
            if job.category_tags:
                # Delete existing associations for this job
                session.query(JobCategoryTag).filter_by(job_id=job.source_job_id).delete()
                
                category_data_to_insert = []
                for cat_id_str in job.category_tags:
                    # CategorySource primary key is source_category_id (string)
                    category_source = session.query(CategorySource).filter_by(source_category_id=cat_id_str).first()
                    if category_source:
                        category_data_to_insert.append({
                            "job_id": job.source_job_id,
                            "category_source_id": category_source.source_category_id
                        })
                
                if category_data_to_insert:
                    try:
                        stmt = insert(JobCategoryTag).values(category_data_to_insert)
                        on_duplicate_stmt = stmt.on_duplicate_key_update(job_id=stmt.inserted.job_id)
                        session.execute(on_duplicate_stmt)
                    except Exception as e:
                        logger.error("Error upserting JobCategoryTag batch within upsert_jobs.", error=str(e), exc_info=True)

        session.commit()
        logger.info(f"Successfully upserted {len(jobs)} jobs and their relations.")


def get_url_by_url_string(url: str, db_name: str = None) -> Optional[UrlPydantic]:
    """
    Retrieves a URL object from the database based on the URL string.
    """
    with get_session(db_name=db_name) as session:
        statement = select(Url).where(Url.source_url == url)
        url_object = session.scalars(statement).first()
        if url_object:
            return UrlPydantic.model_validate(url_object)
        return None

def get_urls_to_process(
    platform: SourcePlatform, statuses: List[CrawlStatus], limit: int, db_name: str = None
) -> List[UrlPydantic]:
    """
    從資料庫獲取指定平台和指定爬取狀態列表的 URL 對象。
    """
    with get_session(db_name=db_name) as session:
        statement = (
            select(Url)
            .where(Url.source == platform, Url.details_crawl_status.in_([s.value for s in statuses]))
            .limit(limit)
        )
        urls = [UrlPydantic.model_validate(u) for u in session.scalars(statement).all()]
        logger.debug(
            "Fetched URLs to process.",
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
            .values(details_crawl_status=status.value, details_crawled_at=now)
        )
        session.execute(stmt)
        logger.info(
            "Successfully updated URL statuses.",
            status=status.value,
            count=len(urls),
        )


def clear_urls_and_categories(db_name: str = None) -> None:
    """
    清空 tb_urls 和 tb_url_categories 資料表。
    """
    with get_session(db_name=db_name) as session:
        session.execute(delete(JobCategoryTag)) # Changed from UrlCategory to JobCategoryTag
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
    with get_session(db_name=db_name) as session:
        affected_rows = _generic_upsert(session, CategorySource, flattened_data, update_cols)

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


def upsert_urls(platform: SourcePlatform, urls: List[UrlPydantic], db_name: str = None) -> None:
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
            "source_url": url.source_url,
            "source": platform,
            "source_category_id": url.source_category_id,
            "status": JobStatus.ACTIVE.value,
            "details_crawl_status": CrawlStatus.QUEUED.value,
            "crawled_at": now,
            "updated_at": now,
        }
        for url in urls
    ]

    update_cols = ["status", "updated_at", "details_crawl_status", "source_category_id"]
    with get_session(db_name=db_name) as session:
        affected_rows = _generic_upsert(session, Url, url_models_to_upsert, update_cols)

    logger.info("URLs upserted successfully.", platform=platform.value, count=len(urls), affected_rows=affected_rows)


def upsert_url_categories(url_category_tags: List[Dict[str, str]], db_name: str = None) -> None:
    """
    Upserts job category tags into the tb_job_category_tags table.
    """
    if not url_category_tags:
        logger.info("No URL category tags to upsert.")
        return

    with get_session(db_name=db_name) as session:
        affected_rows = _generic_upsert(session, JobCategoryTag, url_category_tags, ['job_id'])
        logger.info(f"Successfully upserted {affected_rows} job category tags.")





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
        # JobCategoryTag.job_id is now source_job_id (string)
        # Job.source_job_id is now the primary key (string)
        query = select(JobCategoryTag.category_source_id).join(Job, JobCategoryTag.job_id == Job.source_job_id).where(Job.source_platform == platform)
        df = pd.read_sql(query, session.bind)
        return set(df["category_source_id"].tolist())


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
        # JobCategoryTag.job_id is now source_job_id (string)
        # Job.source_job_id is now the primary key (string)
        query = (
            select(JobCategoryTag.category_source_id)
            .join(Job, JobCategoryTag.job_id == Job.source_job_id)
            .where(
                Job.source_platform == platform,
                Job.updated_at < threshold_date # Use Job.updated_at for staleness
            )
            .distinct()
        )
        df = pd.read_sql(query, session.bind)
        return set(df["category_source_id"].tolist())

def sync_job_observations_geocoding(db_name: str = None, batch_size: int = 1000) -> None:
    """
    同步 tb_job_observations 表中的地理編碼資訊，從 tb_locations 獲取經緯度。
    """
    logger.info("開始同步 tb_job_observations 的地理編碼資訊。")
    total_synced_count = 0

    while True:
        with get_session(db_name=db_name) as session:
            job_observations_to_sync = session.query(JobObservation).filter(
                (JobObservation.latitude.is_(None)) | (JobObservation.latitude == '') |
                (JobObservation.longitude.is_(None)) | (JobObservation.longitude == ''),
                JobObservation.location_text.isnot(None),
                JobObservation.location_text != ''
            ).limit(batch_size).all()

            if not job_observations_to_sync:
                logger.info("沒有更多 tb_job_observations 記錄需要同步地理編碼資訊。")
                break

            logger.info(f"找到 {len(job_observations_to_sync)} 筆 tb_job_observations 記錄需要同步地理編碼資訊。")

            # 批量獲取所有需要的地址
            location_texts = {obs.location_text for obs in job_observations_to_sync if obs.location_text}
            if not location_texts:
                logger.info("此批次中沒有有效的地址可供查詢。")
                continue

            # 一次性查詢所有相關的地理編碼位置
            geocoded_locations_query = session.query(Location).filter(Location.address_detail.in_(location_texts))
            geocoded_locations_map = {loc.address_detail: loc for loc in geocoded_locations_query.all()}

            synced_in_batch = 0
            for obs in job_observations_to_sync:
                geocoded_location = geocoded_locations_map.get(obs.location_text)

                if geocoded_location and geocoded_location.latitude and geocoded_location.longitude:
                    obs.latitude = geocoded_location.latitude
                    obs.longitude = geocoded_location.longitude
                    synced_in_batch += 1
                else:
                    logger.debug(
                        "tb_locations 中未找到地理編碼資訊或資訊不完整。",
                        job_id=obs.source_job_id,
                        address=obs.location_text,
                    )
            
            if synced_in_batch > 0:
                logger.info(f"準備提交 {synced_in_batch} 筆 tb_job_observations 記錄的地理編碼更新。")
                try:
                    session.commit()
                    logger.info(f"已成功提交 {synced_in_batch} 筆 tb_job_observations 記錄的地理編碼更新。")
                    total_synced_count += synced_in_batch
                except Exception as e:
                    session.rollback()
                    logger.error(f"提交 tb_job_observations 地理編碼更新時發生錯誤: {e}", exc_info=True)
            else:
                logger.info("此批次沒有 tb_job_observations 記錄的地理編碼需要更新或提交。")

    logger.info(f"tb_job_observations 地理編碼同步完成。總共更新了 {total_synced_count} 筆記錄。")

