"""HubSpot API router."""

import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.config import settings
from app.core.security import verify_hubspot_signature
from app.modules.hubspot import service
from app.modules.hubspot.schema import HubSpotWebhookEvent
from app.modules.hubspot.service import (
    DEAL_PROPERTIES,
    CONTACT_PROPERTIES,
    COMPANY_PROPERTIES,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/hubspot", tags=["HubSpot"])


# ═══════════════════════════════════════════════════════════════════════════════
# LOAD / SYNC ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/load/start", summary="Start full HubSpot sync (streams live SSE progress)")
async def start_load(
    deal_properties: str = Query(
        default=",".join(DEAL_PROPERTIES),
        description="Comma-separated deal properties to fetch",
    ),
    contact_properties: str = Query(
        default=",".join(CONTACT_PROPERTIES),
        description="Comma-separated contact properties to fetch",
    ),
    company_properties: str = Query(
        default=",".join(COMPANY_PROPERTIES),
        description="Comma-separated company properties to fetch",
    ),
):
    """
    Runs a full HubSpot sync and streams live progress as Server-Sent Events.
    HubSpot access token is read from HUBSPOT_ACCESS_TOKEN env var.
    """
    hubspot_token = settings.HUBSPOT_ACCESS_TOKEN.strip()
    if not hubspot_token:
        raise HTTPException(
            status_code=500,
            detail="HUBSPOT_ACCESS_TOKEN env var is not set.",
        )
    return await service.start_load(
        hubspot_token=hubspot_token,
        deal_properties=deal_properties,
        contact_properties=contact_properties,
        company_properties=company_properties,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# WEBHOOK ENDPOINT
# ═══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/webhook",
    summary="Receive HubSpot webhook events",
    dependencies=[Depends(verify_hubspot_signature)],
    status_code=200,
)
async def hubspot_webhook(
    request: Request,
    events: List[HubSpotWebhookEvent],
):
    """
    Receives and processes HubSpot v3 webhook events.

    Subscription types handled:
      deal.creation / deal.propertyChange / deal.deletion / deal.associationChange
      contact.creation / contact.propertyChange / contact.deletion / contact.associationChange
      company.creation / company.propertyChange / company.deletion / company.associationChange

    Association changes are reflected immediately:
      - associationRemoved=False → row upserted / restored in deal_contacts or deal_companies
      - associationRemoved=True  → row soft-deleted (deleted_at set, row kept for audit)

    Signature is validated before this handler runs (via Depends).
    HubSpot access token is read from HUBSPOT_ACCESS_TOKEN env var.
    """
    hubspot_token = settings.HUBSPOT_ACCESS_TOKEN.strip()
    if not hubspot_token:
        raise HTTPException(
            status_code=500,
            detail="HUBSPOT_ACCESS_TOKEN env var is not set.",
        )
    result = await service.handle_webhook(events, hubspot_token)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/", summary="Health check")
async def root():
    return {"status": "ok", "service": "HubSpot Sync Service"}