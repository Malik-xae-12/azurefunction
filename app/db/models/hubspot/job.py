from sqlalchemy import Column, DateTime, Integer, String, Text

from app.db.base import Base


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String, unique=True, index=True, nullable=False)
    status = Column(String, nullable=False)
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime, nullable=True)

    pages_processed = Column(Integer, default=0)
    deals_fetched = Column(Integer, default=0)
    contacts_fetched = Column(Integer, default=0)
    companies_fetched = Column(Integer, default=0)
    attachments_fetched = Column(Integer, default=0)
    api_calls_made = Column(Integer, default=0)

    error = Column(Text, nullable=True)
    errors_json = Column(Text, nullable=True)
    result_sample_json = Column(Text, nullable=True)
