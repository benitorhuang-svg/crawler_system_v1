from crawler.database.connection import get_session
from crawler.database.models import Job
from crawler.database.schemas import SourcePlatform
from crawler.utils.salary_parser import parse_salary_text
from crawler.project_1111.parser_apidata_1111 import derive_salary_type

try:
    with get_session() as session:
        # Query for jobs that match the original criteria
        # You can modify this filter as needed for actual data updates
        jobs_to_update = session.query(Job).filter(
            Job.source_platform == SourcePlatform.PLATFORM_1111,
            Job.salary_text.isnot(None) # Ensure salary_text is not null
        ).all()

        print(f"Found {len(jobs_to_update)} jobs to process.")

        for job in jobs_to_update:
            min_salary, max_salary = parse_salary_text(job.salary_text)
            # Assuming job.job_type is already populated from the crawler
            salary_type = derive_salary_type(job.salary_text, min_salary, job.job_type)

            if min_salary is not None or salary_type is not None: # Update if either min_salary is parsed or type is derived
                job.salary_min = min_salary
                job.salary_max = max_salary
                job.salary_type = salary_type
                print(f"Updating job_id: {job.id} - salary_text: '{job.salary_text}' - salary_min: {min_salary}, salary_max: {max_salary}, salary_type: {salary_type}")
            else:
                print(f"Skipping job_id: {job.id} - Could not parse salary from '{job.salary_text}'")

        print("Successfully processed salary information for matching jobs.")

except Exception as e:
    print(f"An error occurred: {e}")
