"""Pydantic models for request/response schemas."""

from enum import Enum
from typing import Any, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class LoadStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class LoadProgress(BaseModel):
    job_id: str
    status: LoadStatus = LoadStatus.RUNNING
    pages_processed: int = 0
    deals_fetched: int = 0
    contacts_fetched: int = 0
    companies_fetched: int = 0
    attachments_fetched: int = 0
    api_calls_made: int = 0
    errors: List[str] = Field(default_factory=list)
    error: Optional[str] = None
    started_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    completed_at: Optional[str] = None
    result_sample: List[dict] = Field(
        default_factory=list,
        description="First 3 enriched deals as a preview"
    )


class LoadResult(BaseModel):
    job_id: str
    total_deals: int
    total_contacts: int
    total_companies: int
    total_attachments: int
    api_calls_made: int
    errors: List[str]
    completed_at: str