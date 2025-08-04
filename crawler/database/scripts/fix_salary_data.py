from crawler.database.connection import get_session
from crawler.database.models import Job104, Job1111, JobCakeresume, JobYes123, JobYourator # Import all specific Job models
from crawler.database.schemas import SourcePlatform
from crawler.utils.salary_parser import parse_salary_text
import structlog

logger = structlog.get_logger(__name__)

# Map SourcePlatform to the corresponding Job model
PLATFORM_JOB_MODELS = {
    SourcePlatform.PLATFORM_104: Job104,
    SourcePlatform.PLATFORM_1111: Job1111,
    SourcePlatform.PLATFORM_CAKERESUME: JobCakeresume,
    SourcePlatform.PLATFORM_YES123: JobYes123,
    SourcePlatform.PLATFORM_YOURATOR: JobYourator,
}

def fix_salary_data_for_platform(platform: SourcePlatform, JobModel):
    logger.info(f"Processing salary data for platform: {platform.value}")
    try:
        with get_session() as session:
            # Query for jobs that match the original criteria
            jobs_to_update = session.query(JobModel).filter(
                JobModel.source_platform == platform,
                JobModel.salary_text.isnot(None) # Ensure salary_text is not null
            ).all()

            logger.info(f"Found {len(jobs_to_update)} jobs to process for {platform.value}.")

            for job in jobs_to_update:
                min_salary, max_salary, salary_type_parsed = parse_salary_text(job.salary_text)
                
                # Note: derive_salary_type is specific to 1111. 
                # If other platforms need specific derivation, this logic needs to be generalized or handled per platform.
                # For now, we'll use the parsed salary_type directly from parse_salary_text.
                # If derive_salary_type is truly generic, it should be moved to utils.
                # For this fix, we assume parse_salary_text is sufficient or derive_salary_type is only for 1111.
                # If derive_salary_type is needed for all, it should be refactored.
                # For now, we'll use the salary_type_parsed from parse_salary_text.
                final_salary_type = salary_type_parsed

                if min_salary is not None or final_salary_type is not None: # Update if either min_salary is parsed or type is derived
                    job.salary_min = min_salary
                    job.salary_max = max_salary
                    job.salary_type = final_salary_type
                    logger.debug(f"Updating job_id: {job.id} - salary_text: '{job.salary_text}' - salary_min: {min_salary}, salary_max: {max_salary}, salary_type: {final_salary_type}")
                else:
                    logger.warning(f"Skipping job_id: {job.id} - Could not parse salary from '{job.salary_text}'")
            
            session.commit() # Commit changes for the current platform
            logger.info(f"Successfully processed salary information for {platform.value}.")

    except Exception as e:
        logger.error(f"An error occurred while processing salary data for {platform.value}: {e}", exc_info=True)

if __name__ == "__main__":
    # Ensure the database is initialized before running
    from crawler.database.connection import initialize_database
    initialize_database()

    # Process each platform
    for platform, JobModel in PLATFORM_JOB_MODELS.items():
        fix_salary_data_for_platform(platform, JobModel)