import os
import sys
from sqlalchemy.orm import sessionmaker

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__))))

from crawler.database.connection import initialize_database, get_engine
from crawler.database.models import Job # Import the Job model
from crawler.database.schemas import SourcePlatform
import structlog

structlog.configure(
    processors=[
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger(__name__)

def check_null_values_for_cakeresume():
    logger.info("Initializing database connection...")
    # Ensure the database is initialized for the script to connect
    os.environ['CRAWLER_DB_NAME'] = 'test_db' # Set the database name to test_db
    initialize_database()
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    logger.info("Querying for PLATFORM_CAKERESUME jobs with null values...")

    # Columns to check for nulls
    columns_to_check = [
        "description", "job_type", "location_text", "posted_at",
        "salary_text", "salary_min", "salary_max", "salary_type",
        "experience_required_text", "education_required_text",
        "company_source_id", "company_name", "company_url"
    ]

    null_counts = {col: 0 for col in columns_to_check}
    total_cakeresume_jobs = 0

    try:
        jobs = session.query(Job).filter(Job.source_platform == SourcePlatform.PLATFORM_CAKERESUME).all()
        total_cakeresume_jobs = len(jobs)

        if total_cakeresume_jobs == 0:
            logger.info("No jobs found for PLATFORM_CAKERESUME.")
            return

        for job in jobs:
            for col in columns_to_check:
                if getattr(job, col) is None:
                    null_counts[col] += 1
        
        logger.info(f"Total PLATFORM_CAKERESUME jobs: {total_cakeresume_jobs}")
        logger.info("Null value counts for PLATFORM_CAKERESUME jobs:")
        for col, count in null_counts.items():
            if count > 0:
                logger.info(f"  {col}: {count} ({(count/total_cakeresume_jobs)*100:.2f}%) of total jobs")
            else:
                logger.info(f"  {col}: No null values")

    except Exception as e:
        logger.error("An error occurred during database query.", error=e, exc_info=True)
    finally:
        session.close()

if __name__ == "__main__":
    check_null_values_for_cakeresume()