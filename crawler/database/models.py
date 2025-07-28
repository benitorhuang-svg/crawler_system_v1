from sqlalchemy import Column, Integer, String, Text, DateTime, Enum
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import enum
from typing import Optional
from pydantic import BaseModel

Base = declarative_base()

class SourcePlatform(enum.Enum):
    PLATFORM_104 = "104"
    PLATFORM_1111 = "1111"
    PLATFORM_CAKEHR = "cakehr"

class JobStatus(enum.Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"

class CrawlStatus(enum.Enum):
    PENDING = "pending"
    CRAWLED = "crawled"
    FAILED = "failed"

class JobType(enum.Enum):
    FULL_TIME = "full_time"
    PART_TIME = "part_time"
    CONTRACT = "contract"
    INTERNSHIP = "internship"

class SalaryType(enum.Enum):
    MONTHLY = "monthly"
    YEARLY = "yearly"
    HOURLY = "hourly"
    DAILY = "daily"

# SQLAlchemy Models
class CategorySource(Base):
    __tablename__ = "tb_category_source"
    id = Column(Integer, primary_key=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False)
    source_category_id = Column(String(255), nullable=False)
    source_category_name = Column(String(255), nullable=False)
    parent_source_id = Column(String(255))

class Url(Base):
    __tablename__ = "tb_urls"
    source_url = Column(String(512), primary_key=True)
    source = Column(Enum(SourcePlatform), nullable=False, index=True)
    status = Column(Enum(JobStatus), nullable=False, index=True, default=JobStatus.ACTIVE)
    details_crawl_status = Column(Enum(CrawlStatus), nullable=False, index=True, default=CrawlStatus.PENDING)
    crawled_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    details_crawled_at = Column(DateTime)

class Job(Base):
    __tablename__ = "tb_jobs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False, index=True)
    source_job_id = Column(String(255), index=True, nullable=False)
    url = Column(String(512), index=True, nullable=False)
    status = Column(Enum(JobStatus), nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text)
    job_type = Column(Enum(JobType))
    location_text = Column(String(255))
    posted_at = Column(DateTime)
    salary_text = Column(String(255))
    salary_min = Column(Integer)
    salary_max = Column(Integer)
    salary_type = Column(Enum(SalaryType))
    experience_required_text = Column(String(255))
    education_required_text = Column(String(255))
    company_source_id = Column(String(255))
    company_name = Column(String(255))
    company_url = Column(String(512))
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

# Pydantic Models
class CategorySourcePydantic(BaseModel):
    id: Optional[int] = None
    source_platform: SourcePlatform
    source_category_id: str
    source_category_name: str
    parent_source_id: Optional[str] = None

    class Config:
        from_attributes = True

class UrlPydantic(BaseModel):
    source_url: str
    source: SourcePlatform
    status: JobStatus = JobStatus.ACTIVE
    details_crawl_status: CrawlStatus = CrawlStatus.PENDING
    crawled_at: datetime = datetime.utcnow()
    updated_at: datetime = datetime.utcnow()
    details_crawled_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class JobPydantic(BaseModel):
    id: Optional[int] = None
    source_platform: SourcePlatform
    source_job_id: str
    url: str
    status: JobStatus
    title: str
    description: Optional[str] = None
    job_type: Optional[JobType] = None
    location_text: Optional[str] = None
    posted_at: Optional[datetime] = None
    salary_text: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_type: Optional[SalaryType] = None
    experience_required_text: Optional[str] = None
    education_required_text: Optional[str] = None
    company_source_id: Optional[str] = None
    company_name: Optional[str] = None
    company_url: Optional[str] = None
    created_at: datetime = datetime.utcnow()
    updated_at: datetime = datetime.utcnow()

    class Config:
        from_attributes = True