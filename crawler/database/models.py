from sqlalchemy import Column, Integer, String, Text, DateTime, Enum
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone  # Import timezone
import enum
from typing import Optional
from pydantic import BaseModel, Field  # Import Field

Base = declarative_base()


class SourcePlatform(str, enum.Enum):
    """資料來源平台。用於在資料庫中標識數據的來源。"""

    PLATFORM_104 = "platform_104"
    PLATFORM_1111 = "platform_1111"
    PLATFORM_CAKERESUME = "platform_cakeresume"
    PLATFORM_YES123 = "platform_yes123"


class JobStatus(str, enum.Enum):
    """職缺或 URL 的活躍狀態。"""

    ACTIVE = "active"
    INACTIVE = "inactive"


class CrawlStatus(str, enum.Enum):
    """職缺詳情頁的抓取狀態。"""

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class SalaryType(str, enum.Enum):
    """標準化的薪資給付週期。"""

    MONTHLY = "MONTHLY"
    HOURLY = "HOURLY"
    YEARLY = "YEARLY"
    DAILY = "DAILY"
    BY_CASE = "BY_CASE"
    NEGOTIABLE = "NEGOTIABLE"


class JobType(str, enum.Enum):
    """標準化的工作類型。"""

    FULL_TIME = "FULL_TIME"
    PART_TIME = "PART_TIME"
    CONTRACT = "CONTRACT"
    INTERNSHIP = "INTERNSHIP"
    TEMPORARY = "TEMPORARY"


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
    status = Column(
        Enum(JobStatus), nullable=False, index=True, default=JobStatus.ACTIVE
    )
    details_crawl_status = Column(
        String(20), nullable=False, index=True, default=CrawlStatus.PENDING.value
    )
    crawled_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )  # Use timezone-aware datetime
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )  # Use timezone-aware datetime and onupdate
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
    job_type = Column(Enum(JobType))  # Changed to Enum(JobType)
    location_text = Column(String(255))
    posted_at = Column(DateTime)
    salary_text = Column(String(255))
    salary_min = Column(Integer)
    salary_max = Column(Integer)
    salary_type = Column(Enum(SalaryType))  # Changed to Enum(SalaryType)
    experience_required_text = Column(String(255))
    education_required_text = Column(String(255))
    company_source_id = Column(String(255))
    company_name = Column(String(255))
    company_url = Column(String(512))
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )  # Use timezone-aware datetime
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )  # Use timezone-aware datetime and onupdate


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
    crawled_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )  # Use default_factory
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )  # Use default_factory
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
    job_type: Optional[JobType] = None  # Changed to Optional[JobType]
    location_text: Optional[str] = None
    posted_at: Optional[datetime] = None
    salary_text: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_type: Optional[SalaryType] = None  # Changed to Optional[SalaryType]
    experience_required_text: Optional[str] = None
    education_required_text: Optional[str] = None
    company_source_id: Optional[str] = None
    company_name: Optional[str] = None
    company_url: Optional[str] = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )  # Use default_factory
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )  # Use default_factory

    class Config:
        from_attributes = True
