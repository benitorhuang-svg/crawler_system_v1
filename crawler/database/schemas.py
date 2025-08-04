from datetime import datetime, timezone
from typing import Optional
import enum

from pydantic import BaseModel, Field


class SourcePlatform(str, enum.Enum):
    """資料來源平台。用於在資料庫中標識數據的來源。"""

    PLATFORM_104 = "platform_104"
    PLATFORM_1111 = "platform_1111"
    PLATFORM_CAKERESUME = "platform_cakeresume"
    PLATFORM_YES123 = "platform_yes123"
    PLATFORM_YOURATOR = "platform_yourator"


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
    OTHER = "OTHER"


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
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    details_crawled_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class UrlCategoryPydantic(BaseModel):
    source_url: str
    source_category_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

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
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    class Config:
        from_attributes = True


class JobLocationPydantic(BaseModel):
    id: Optional[int] = None
    source_platform: SourcePlatform
    source_job_id: str
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    class Config:
        from_attributes = True
