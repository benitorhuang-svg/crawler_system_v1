from sqlalchemy import Column, Integer, String, Text, DateTime, Enum, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone

from crawler.database.schemas import (
    SourcePlatform,
    JobStatus,
    CrawlStatus,
    SalaryType,
    JobType,
)

Base = declarative_base()


# SQLAlchemy Models
class CategorySource(Base):
    __tablename__ = "tb_category_source"
    id = Column(Integer, primary_key=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False)
    source_category_id = Column(String(255), nullable=False, unique=True)
    source_category_name = Column(String(255), nullable=False)
    parent_source_id = Column(String(255))

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
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    details_crawled_at = Column(DateTime)

    category_associations = relationship("UrlCategory", back_populates="url")


class UrlCategory(Base):
    __tablename__ = "tb_url_categories"
    source_url = Column(String(512), ForeignKey("tb_urls.source_url"), primary_key=True)
    source_category_id = Column(String(255), ForeignKey("tb_category_source.source_category_id"), primary_key=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    url = relationship("Url", back_populates="category_associations")
    category = relationship("CategorySource", back_populates="url_associations")


# Define Job models for each platform
class Job104(Base):
    __tablename__ = "tb_jobs_104"
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
    __table_args__ = (UniqueConstraint('source_platform', 'url', name='_source_platform_url_uc_104'),)

class Job1111(Base):
    __tablename__ = "tb_jobs_1111"
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
    __table_args__ = (UniqueConstraint('source_platform', 'url', name='_source_platform_url_uc_1111'),)

class JobCakeresume(Base):
    __tablename__ = "tb_jobs_cakeresume"
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
    __table_args__ = (UniqueConstraint('source_platform', 'url', name='_source_platform_url_uc_cakeresume'),)

class JobYes123(Base):
    __tablename__ = "tb_jobs_yes123"
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
    __table_args__ = (UniqueConstraint('source_platform', 'url', name='_source_platform_url_uc_yes123'),)

class JobYourator(Base):
    __tablename__ = "tb_jobs_yourator"
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
    __table_args__ = (UniqueConstraint('source_platform', 'url', name='_source_platform_url_uc_yourator'),)


# Define JobLocation models for each platform
class JobLocation104(Base):
    __tablename__ = "tb_job_locations_104"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False, index=True)
    source_job_id = Column(String(255), nullable=False, index=True)
    latitude = Column(String(255))
    longitude = Column(String(255))
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    __table_args__ = (UniqueConstraint('source_platform', 'source_job_id', name='_source_platform_job_id_uc_104'),)

class JobLocation1111(Base):
    __tablename__ = "tb_job_locations_1111"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False, index=True)
    source_job_id = Column(String(255), nullable=False, index=True)
    latitude = Column(String(255))
    longitude = Column(String(255))
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    __table_args__ = (UniqueConstraint('source_platform', 'source_job_id', name='_source_platform_job_id_uc_1111'),)

class JobLocationCakeresume(Base):
    __tablename__ = "tb_job_locations_cakeresume"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False, index=True)
    source_job_id = Column(String(255), nullable=False, index=True)
    latitude = Column(String(255))
    longitude = Column(String(255))
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    __table_args__ = (UniqueConstraint('source_platform', 'source_job_id', name='_source_platform_job_id_uc_cakeresume'),)

class JobLocationYes123(Base):
    __tablename__ = "tb_job_locations_yes123"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False, index=True)
    source_job_id = Column(String(255), nullable=False, index=True)
    latitude = Column(String(255))
    longitude = Column(String(255))
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    __table_args__ = (UniqueConstraint('source_platform', 'source_job_id', name='_source_platform_job_id_uc_yes123'),)

class JobLocationYourator(Base):
    __tablename__ = "tb_job_locations_yourator"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False, index=True)
    source_job_id = Column(String(255), nullable=False, index=True)
    latitude = Column(String(255))
    longitude = Column(String(255))
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    __table_args__ = (UniqueConstraint('source_platform', 'source_job_id', name='_source_platform_job_id_uc_yourator'),)