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
    source_category_id = Column(String(255), primary_key=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False)
    source_category_name = Column(String(255), nullable=False)
    parent_source_id = Column(String(255))

    job_associations = relationship("JobCategoryTag", back_populates="category")


class Url(Base):
    __tablename__ = "tb_urls"
    source_url = Column(String(512), primary_key=True)
    source = Column(Enum(SourcePlatform), nullable=False, index=True)
    source_category_id = Column(String(255), ForeignKey("tb_category_source.source_category_id"), nullable=True)
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


class Company(Base):
    __tablename__ = "tb_companies"
    source_company_id = Column(String(255), primary_key=True, index=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False)
    name = Column(String(255), nullable=False)
    url = Column(String(512), nullable=True)
    
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    
    jobs = relationship("Job", back_populates="company")


class Location(Base):
    __tablename__ = "tb_locations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    region = Column(String(255), nullable=True)
    district = Column(String(255), nullable=True)
    address_detail = Column(String(512), nullable=True)
    latitude = Column(String(255), nullable=True)
    longitude = Column(String(255), nullable=True)

    job_associations = relationship("JobLocation", back_populates="location")

    __table_args__ = (UniqueConstraint('address_detail', name='_address_detail_uc'),)


class Skill(Base):
    __tablename__ = "tb_skills"
    name = Column(String(255), primary_key=True)

    job_associations = relationship("JobSkill", back_populates="skill")


class Job(Base):
    __tablename__ = "tb_jobs"
    source_job_id = Column(String(255), primary_key=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False, index=True)
    url = Column(String(512), index=True, nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    job_type = Column(Enum(JobType), nullable=True)
    posted_at = Column(DateTime, nullable=True)
    status = Column(Enum(JobStatus), nullable=False)
    salary_text = Column(String(255), nullable=True)
    salary_min = Column(Integer, nullable=True)
    salary_max = Column(Integer, nullable=True)
    salary_type = Column(Enum(SalaryType), nullable=True)
    experience_required_text = Column(String(255), nullable=True)
    education_required_text = Column(String(255), nullable=True)
    company_id = Column(String(255), ForeignKey("tb_companies.source_company_id"), nullable=False)
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    company = relationship("Company", back_populates="jobs")
    location_associations = relationship("JobLocation", back_populates="job")
    skill_associations = relationship("JobSkill", back_populates="job")
    category_associations = relationship("JobCategoryTag", back_populates="job")


class JobLocation(Base):
    __tablename__ = "tb_job_locations"
    job_id = Column(String(255), ForeignKey("tb_jobs.source_job_id"), primary_key=True)
    location_id = Column(Integer, ForeignKey("tb_locations.id"), primary_key=True)

    job = relationship("Job", back_populates="location_associations")
    location = relationship("Location", back_populates="job_associations")


class JobSkill(Base):
    __tablename__ = "tb_job_skills"
    job_id = Column(String(255), ForeignKey("tb_jobs.source_job_id"), primary_key=True)
    skill_id = Column(String(255), ForeignKey("tb_skills.name"), primary_key=True)

    job = relationship("Job", back_populates="skill_associations")
    skill = relationship("Skill", back_populates="job_associations")


class JobCategoryTag(Base):
    __tablename__ = "tb_job_category_tags"
    job_id = Column(String(255), ForeignKey("tb_jobs.source_job_id"), primary_key=True)
    category_source_id = Column(String(255), ForeignKey("tb_category_source.source_category_id"), primary_key=True)

    job = relationship("Job", back_populates="category_associations")
    category = relationship("CategorySource", back_populates="job_associations")


class JobObservation(Base):
    __tablename__ = "tb_job_observations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_job_id = Column(String(255), index=True, nullable=False)
    source_platform = Column(Enum(SourcePlatform), nullable=False, index=True)
    url = Column(String(512), nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    job_type = Column(Enum(JobType), nullable=True)
    posted_at = Column(DateTime, nullable=True)
    status = Column(Enum(JobStatus), nullable=False)
    salary_text = Column(String(255), nullable=True)
    salary_min = Column(Integer, nullable=True)
    salary_max = Column(Integer, nullable=True)
    salary_type = Column(Enum(SalaryType), nullable=True)
    experience_required_text = Column(String(255), nullable=True)
    education_required_text = Column(String(255), nullable=True)
    company_id = Column(String(255), nullable=True) # No foreign key constraint here to allow for more flexibility
    company_name = Column(String(255), nullable=True)
    company_url = Column(String(512), nullable=True)
    location_text = Column(String(512), nullable=True)
    region = Column(String(255), nullable=True)
    district = Column(String(255), nullable=True)
    latitude = Column(String(255), nullable=True)
    longitude = Column(String(255), nullable=True)
    skills = Column(Text, nullable=True)
    observed_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
