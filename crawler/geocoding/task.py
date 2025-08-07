import structlog

from crawler.worker import app
from crawler.database.models import SourcePlatform
from crawler.database.schemas import LocationPydantic
from crawler.database.repository import upsert_locations, upsert_job_location_association
from crawler.database.connection import get_session
from crawler.geocoding.client import geocode_address

logger = structlog.get_logger(__name__)


@app.task()
def geocode_job_location(
    source_platform: str, source_job_id: str, location_text: str
) -> None:
    """
    Celery 任務：接收職缺的地理位置文字，呼叫地理編碼服務，
    並將經緯度資訊儲存到資料庫。
    """
    logger.info(
        "Starting geocoding for job location.",
        source_platform=source_platform,
        source_job_id=source_job_id,
        location_text=location_text,
    )

    try:
        geocoded_data = geocode_address(location_text)

        if geocoded_data:
            # 只有當任務不是在 eager 模式下執行時才寫入資料庫
            if not app.conf.task_always_eager:
                with get_session() as session:
                    # 創建 LocationPydantic 對象
                    location_pydantic = LocationPydantic(
                        address_detail=location_text,
                        latitude=str(geocoded_data["latitude"]),
                        longitude=str(geocoded_data["longitude"]),
                    )
                    
                    # 插入或更新地點資訊，並獲取 location_id
                    location_map = upsert_locations(session, [location_pydantic])
                    location_id = location_map.get(location_text)

                    if location_id:
                        # 建立職位與地點的關聯
                        upsert_job_location_association(session, source_job_id, location_id)
                        session.commit() # Commit the changes for both upsert_locations and upsert_job_location_association
                        logger.info(
                            "Geocoding successful and data upserted.",
                            source_platform=source_platform,
                            source_job_id=source_job_id,
                            location_text=location_text,
                            latitude=geocoded_data["latitude"],
                            longitude=geocoded_data["longitude"],
                            location_id=location_id,
                        )
                    else:
                        logger.warning(
                            "Geocoding successful but failed to get location_id for upsert.",
                            source_platform=source_platform,
                            source_job_id=source_job_id,
                            location_text=location_text,
                        )
            else:
                logger.info(
                    "Geocoding successful (eager mode), skipping database upsert.",
                    source_platform=source_platform,
                    source_job_id=source_job_id,
                    location_text=location_text,
                    latitude=geocoded_data["latitude"],
                    longitude=geocoded_data["longitude"],
                )
        else:
            logger.warning(
                "Geocoding failed for location.",
                source_platform=source_platform,
                source_job_id=source_job_id,
                location_text=location_text,
            )

    except Exception as e:
        logger.error(
            "An unexpected error occurred during geocoding task.",
            source_platform=source_platform,
            source_job_id=source_job_id,
            location_text=location_text,
            error=e,
            exc_info=True,
        )


if __name__ == "__main__":
    from crawler.database.connection import initialize_database, get_session
    from crawler.database.schemas import JobPydantic, JobStatus, JobType, SourcePlatform
    from crawler.database.repository import upsert_jobs
    from crawler.database.models import Job, JobLocation
    from sqlalchemy import delete
    from datetime import datetime, timezone

    initialize_database()

    test_jobs_data = [
        {
            "source_platform": SourcePlatform.PLATFORM_104,
            "source_job_id": "test_job_1",
            "url": "http://example.com/job/1",
            "title": "測試職位1",
            "description": "這是測試職位1的描述",
            "job_type": JobType.FULL_TIME,
            "posted_at": datetime.now(timezone.utc),
            "status": JobStatus.ACTIVE,
        },
        {
            "source_platform": SourcePlatform.PLATFORM_1111,
            "source_job_id": "test_job_2",
            "url": "http://example.com/job/2",
            "title": "測試職位2",
            "description": "這是測試職位2的描述",
            "job_type": JobType.PART_TIME,
            "posted_at": datetime.now(timezone.utc),
            "status": JobStatus.ACTIVE,
        },
        {
            "source_platform": SourcePlatform.PLATFORM_CAKERESUME,
            "source_job_id": "test_job_3",
            "url": "http://example.com/job/3",
            "title": "測試職位3",
            "description": "這是測試職位3的描述",
            "job_type": JobType.CONTRACT,
            "posted_at": datetime.now(timezone.utc),
            "status": JobStatus.ACTIVE,
        },
        {
            "source_platform": SourcePlatform.PLATFORM_YES123,
            "source_job_id": "test_job_4",
            "url": "http://example.com/job/4",
            "title": "測試職位4",
            "description": "這是測試職位4的描述",
            "job_type": JobType.INTERNSHIP,
            "posted_at": datetime.now(timezone.utc),
            "status": JobStatus.ACTIVE,
        },
    ]

    def cleanup_test_data():
        with get_session() as session:
            # Delete from child table first (JobLocation) to avoid foreign key constraint issues
            session.execute(delete(JobLocation).where(JobLocation.job_id.in_([job["source_job_id"] for job in test_jobs_data])))
            # Then delete from parent table (Job)
            session.execute(delete(Job).where(Job.source_job_id.in_([job["source_job_id"] for job in test_jobs_data])))
            session.commit()
            logger.info("Cleaned up test job data.")

    # 清理舊的測試資料
    cleanup_test_data()

    # 插入測試職位資料
    jobs_to_upsert = [JobPydantic(**job_data) for job_data in test_jobs_data]
    upsert_jobs(jobs_to_upsert)
    logger.info("Inserted test job data.")

    # 測試範例
    geocode_job_location("platform_104", "test_job_1", "台北市")
    geocode_job_location("platform_1111", "test_job_2", "台中市")
    geocode_job_location("platform_cakeresume", "test_job_3", "高雄市")
    geocode_job_location("platform_yes123", "test_job_4", "未知地點")

    # 清理測試資料
    cleanup_test_data()
