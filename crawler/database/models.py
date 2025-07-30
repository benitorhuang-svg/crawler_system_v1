from sqlalchemy import Column, Integer, String, Text, DateTime, Enum, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship # Import relationship
from datetime import datetime, timezone  # Import timezone
import enum
from typing import Optional, List
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
    source_category_id = Column(String(255), nullable=False, unique=True) # Make unique for relationship
    source_category_name = Column(String(255), nullable=False)
    parent_source_id = Column(String(255))

    # Relationship to UrlCategory
    url_associations = relationship("UrlCategory", back_populates="category")


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

    # Relationship to UrlCategory
    category_associations = relationship("UrlCategory", back_populates="url")


class UrlCategory(Base):
    __tablename__ = "tb_url_categories"
    source_url = Column(String(512), ForeignKey("tb_urls.source_url"), primary_key=True)
    source_category_id = Column(String(255), ForeignKey("tb_category_source.source_category_id"), primary_key=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships to parent tables
    url = relationship("Url", back_populates="category_associations")
    category = relationship("CategorySource", back_populates="url_associations")


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
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationships for extracted data
    skills = relationship("JobSkill", back_populates="job", cascade="all, delete-orphan")
    languages = relationship("JobLanguageAbility", back_populates="job", cascade="all, delete-orphan")
    licenses = relationship("JobLicense", back_populates="job", cascade="all, delete-orphan")


class Skill(Base):
    __tablename__ = "lk_skills"  # Lookup table for skills
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)

    job_associations = relationship("JobSkill", back_populates="skill")


class Language(Base):
    __tablename__ = "lk_languages"  # Lookup table for languages
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)

    job_associations = relationship("JobLanguageAbility", back_populates="language")


class License(Base):
    __tablename__ = "lk_licenses"  # Lookup table for licenses
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)

    job_associations = relationship("JobLicense", back_populates="license")


class JobSkill(Base):
    __tablename__ = "tb_job_skills"
    job_id = Column(Integer, ForeignKey("tb_jobs.id"), primary_key=True)
    skill_id = Column(Integer, ForeignKey("lk_skills.id"), primary_key=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    job = relationship("Job", back_populates="skills")
    skill = relationship("Skill", back_populates="job_associations")


class JobLanguageAbility(Base):
    __tablename__ = "tb_job_language_abilities"
    job_id = Column(Integer, ForeignKey("tb_jobs.id"), primary_key=True)
    language_id = Column(Integer, ForeignKey("lk_languages.id"), primary_key=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    job = relationship("Job", back_populates="languages")
    language = relationship("Language", back_populates="job_associations")


class JobLicense(Base):
    __tablename__ = "tb_job_licenses"
    job_id = Column(Integer, ForeignKey("tb_jobs.id"), primary_key=True)
    license_id = Column(Integer, ForeignKey("lk_licenses.id"), primary_key=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    job = relationship("Job", back_populates="licenses")
    license = relationship("License", back_populates="job_associations")


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


class SkillPydantic(BaseModel):
    id: Optional[int] = None
    name: str

    class Config:
        from_attributes = True


class LanguagePydantic(BaseModel):
    id: Optional[int] = None
    name: str

    class Config:
        from_attributes = True


class LicensePydantic(BaseModel):
    id: Optional[int] = None
    name: str

    class Config:
        from_attributes = True


class JobSkillPydantic(BaseModel):
    job_id: int
    skill_id: int
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        from_attributes = True


class JobLanguageAbilityPydantic(BaseModel):
    job_id: int
    language_id: int
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        from_attributes = True


class JobLicensePydantic(BaseModel):
    job_id: int
    license_id: int
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

    # Fields for extracted data
    extracted_skills: List[SkillPydantic] = []
    extracted_languages: List[LanguagePydantic] = []
    extracted_licenses: List[LicensePydantic] = []

    class Config:
        from_attributes = True
