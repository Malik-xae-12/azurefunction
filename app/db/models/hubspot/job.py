"""Job ORM model — tracks HubSpot sync jobs."""

from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.sql import func

from app.db.base import Base


class Job(Base):
    __tablename__ = "hubspot_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(128), unique=True, nullable=False, index=True)
    status = Column(String(32), nullable=False, default="running")

    pages_processed = Column(Integer, nullable=False, default=0)
    deals_fetched = Column(Integer, nullable=False, default=0)
    contacts_fetched = Column(Integer, nullable=False, default=0)
    companies_fetched = Column(Integer, nullable=False, default=0)
    attachments_fetched = Column(Integer, nullable=False, default=0)
    api_calls_made = Column(Integer, nullable=False, default=0)

    error = Column(Text, nullable=True)
    errors_json = Column(Text, nullable=True, comment="JSON array of non-fatal error strings")
    result_sample_json = Column(Text, nullable=True, comment="JSON array — first 3 deals preview")

    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow, server_default=func.now())

    def __repr__(self) -> str:
        return f"<Job job_id={self.job_id!r} status={self.status!r}>"