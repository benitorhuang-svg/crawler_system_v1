from datetime import datetime, timezone
from typing import Optional, List
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
    source_platform: SourcePlatform
    source_category_id: str
    source_category_name: str
    parent_source_id: Optional[str] = None

    class Config:
        from_attributes = True


class UrlPydantic(BaseModel):
    source_url: str
    source: SourcePlatform
    source_category_id: Optional[str] = None
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


class CompanyPydantic(BaseModel):
    source_platform: SourcePlatform
    source_company_id: str
    name: str
    url: Optional[str] = None
    
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        from_attributes = True


class LocationPydantic(BaseModel):
    id: Optional[int] = None
    region: Optional[str] = None
    district: Optional[str] = None
    address_detail: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None

    class Config:
        from_attributes = True


class SkillPydantic(BaseModel):
    name: str

    class Config:
        from_attributes = True


class JobPydantic(BaseModel):
    source_platform: SourcePlatform
    source_job_id: str
    url: str
    title: str
    description: Optional[str] = None
    job_type: Optional[JobType] = None
    posted_at: Optional[datetime] = None
    status: JobStatus = JobStatus.ACTIVE
    salary_text: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_type: Optional[SalaryType] = None
    experience_required_text: Optional[str] = None
    education_required_text: Optional[str] = None
    company_id: Optional[str] = None # Foreign Key

    # These fields will hold the related objects during processing
    company: Optional[CompanyPydantic] = None
    locations: List[LocationPydantic] = []
    skills: List[SkillPydantic] = []
    category_tags: List[str] = [] # List of source_category_id strings

    class Config:
        from_attributes = True


class JobLocationPydantic(BaseModel):
    job_id: str
    location_id: int

    class Config:
        from_attributes = True


class JobSkillPydantic(BaseModel):
    job_id: str
    skill_id: str

    class Config:
        from_attributes = True


class JobCategoryTagPydantic(BaseModel):
    job_id: str
    category_source_id: str

    class Config:
        from_attributes = True


class JobObservationPydantic(BaseModel):
    source_job_id: str
    source_platform: SourcePlatform
    url: str
    title: str
    description: Optional[str] = None
    job_type: Optional[JobType] = None
    posted_at: Optional[datetime] = None
    status: JobStatus = JobStatus.ACTIVE
    salary_text: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_type: Optional[SalaryType] = None
    experience_required_text: Optional[str] = None
    education_required_text: Optional[str] = None
    company_id: Optional[str] = None
    company_name: Optional[str] = None
    company_url: Optional[str] = None
    location_text: Optional[str] = None
    region: Optional[str] = None
    district: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    skills: Optional[str] = None
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        from_attributes = True
