import structlog

from crawler.worker import app
from crawler.database.models import SourcePlatform, JobLocationPydantic
from crawler.database.repository import upsert_job_locations
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
                job_location_pydantic = JobLocationPydantic(
                    source_platform=SourcePlatform(source_platform),
                    source_job_id=source_job_id,
                    latitude=str(geocoded_data["latitude"]),
                    longitude=str(geocoded_data["longitude"]),
                )
                upsert_job_locations([job_location_pydantic])
                logger.info(
                    "Geocoding successful and data upserted.",
                    source_platform=source_platform,
                    source_job_id=source_job_id,
                    latitude=geocoded_data["latitude"],
                    longitude=geocoded_data["longitude"],
                )
            else:
                logger.info(
                    "Geocoding successful (eager mode), skipping database upsert.",
                    source_platform=source_platform,
                    source_job_id=source_job_id,
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
    from crawler.database.connection import initialize_database
    initialize_database()

    # 測試範例
    geocode_job_location("platform_104", "test_job_1", "台北市")
    geocode_job_location("platform_1111", "test_job_2", "台中市")
    geocode_job_location("platform_cakeresume", "test_job_3", "高雄市")
    geocode_job_location("platform_yes123", "test_job_4", "未知地點")
