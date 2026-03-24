"""Pydantic schemas — HubSpot webhook events and load progress."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ═══════════════════════════════════════════════════════════════════════════════
# LOAD JOB
# ═══════════════════════════════════════════════════════════════════════════════

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
    result_sample: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="First 3 enriched deals as a preview",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# HUBSPOT WEBHOOK EVENT (v3 Private App webhooks)
# ═══════════════════════════════════════════════════════════════════════════════

class HubSpotWebhookEvent(BaseModel):
    """
    Represents a single v3 HubSpot webhook event.

    Subscription types handled:
        deal.creation
        deal.propertyChange      → propertyName / propertyValue populated
        deal.deletion
        deal.associationChange   → associationType / fromObjectId / toObjectId / associationRemoved

        contact.creation
        contact.propertyChange
        contact.deletion
        contact.associationChange

        company.creation
        company.propertyChange
        company.deletion
        company.associationChange

    Reference: https://developers.hubspot.com/docs/api/webhooks

    NOTE: objectId is absent on associationChange events — those carry
    fromObjectId / toObjectId instead.  Making it Optional prevents the
    422 Unprocessable Content that was crashing every association webhook.
    """

    # Always present
    appId: int
    eventId: int
    subscriptionId: int
    portalId: int
    occurredAt: float           # epoch milliseconds
    subscriptionType: str       # e.g. "deal.creation"
    attemptNumber: int

    # FIX: was `int` (required) — associationChange events omit this field entirely,
    # which caused Pydantic to reject the whole request with 422 before any handler ran.
    objectId: Optional[int] = None

    # Property change events
    changeSource: Optional[str] = None
    sourceId: Optional[str] = None          # FIX: was missing — HubSpot sends on every event
    propertyName: Optional[str] = None      # e.g. "dealname", "amount", "dealstage"
    propertyValue: Optional[str] = None     # new value (string)

    # Association change events
    changeFlag: Optional[str] = None        # "CREATED" | "DELETED" (older format)
    associationType: Optional[str] = None   # e.g. "DEAL_TO_CONTACT"
    fromObjectId: Optional[int] = None
    toObjectId: Optional[int] = None
    associationRemoved: Optional[bool] = None
    isPrimaryAssociation: Optional[bool] = None