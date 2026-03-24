"""HubSpot API router."""

import logging
from typing import List

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request

from app.core.security import verify_hubspot_signature
from app.modules.hubspot import service
from app.modules.hubspot.schema import HubSpotWebhookEvent

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/hubspot", tags=["HubSpot"])


# ═══════════════════════════════════════════════════════════════════════════════
# LOAD / SYNC ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/load/start", summary="Start full HubSpot sync (streams live SSE progress)")
async def start_load(
    hubspot_token: str = Header(
        ...,
        alias="x-hubspot-token",
        description="HubSpot Private App access token — never put secrets in query params",
    ),
    deal_properties: str = Query(
        default=(
            "dealname,amount,dealstage,closedate,pipeline,hubspot_owner_id,"
            "deal_owner_email,delivery_owner,project_start_date,project_end_date,"
            "po_hours,dealtype,description"
        ),
        description="Comma-separated deal properties to fetch",
    ),
    contact_properties: str = Query(
        default="firstname,lastname,email,phone,mobilephone,city,country,company,closedate",
        description="Comma-separated contact properties to fetch",
    ),
    company_properties: str = Query(
        default="name,domain,industry,city,country",
        description="Comma-separated company properties to fetch",
    ),
):
    """
    Runs a full HubSpot sync and streams live progress as Server-Sent Events.
    Azure Functions safe — uses async generator, no BackgroundTasks.
    """
    return await service.start_load(
        hubspot_token=hubspot_token,
        deal_properties=deal_properties,
        contact_properties=contact_properties,
        company_properties=company_properties,
    )


@router.get("/load/status/{job_id}", summary="Check job status")
async def get_status(job_id: str):
    """Returns current progress from memory cache or DB fallback."""
    return await service.get_status(job_id)


@router.get("/load/result/{job_id}", summary="Get completed job result")
async def get_result(job_id: str):
    """Returns the final result of a completed sync job."""
    return await service.get_result(job_id)


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
    hubspot_token: str = Header(
        ...,
        alias="x-hubspot-token",
        description="HubSpot Private App access token — needed to fetch updated records",
    ),
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
    """
    result = await service.handle_webhook(events, hubspot_token)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/", summary="Health check")
async def root():
    return {"status": "ok", "service": "HubSpot Sync Service"}