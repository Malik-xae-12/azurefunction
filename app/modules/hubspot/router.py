from fastapi import APIRouter, Query
from app.modules.hubspot import service
from typing import List
from app.modules.hubspot.schema import HubSpotWebhookEvent

router = APIRouter(prefix="/hubspot")  

@router.post("/load/start", summary="Start HubSpot sync (streams LIVE progress)")
async def start_load(
    hubspot_token: str,
    deal_properties: str = Query(
        "dealname,amount,dealstage,closedate,pipeline,hubspot_owner_id,"
        "deal_owner_email,delivery_owner,project_start_date,project_end_date,po_hours"
    ),
    contact_properties: str = Query("firstname,lastname,email,phone"),
    company_properties: str = Query("name,domain,industry,city"),
):
    """Streams live progress - no BackgroundTasks needed!"""
    return await service.start_load(
        hubspot_token=hubspot_token,
        deal_properties=deal_properties,
        contact_properties=contact_properties,
        company_properties=company_properties,
    )

@router.get("/load/status/{job_id}", summary="📊 Check job status")
async def get_status(job_id: str):
    return await service.get_status(job_id)

@router.get("/load/result/{job_id}", summary="✅ Get completed results")
async def get_result(job_id: str):
    return await service.get_result(job_id)

@router.get("/", summary="Health check")
async def root():
    return {"status": "ok", "service": "HubSpot Loader", "docs": "/hubspot/docs"}


@router.post("/webhook", summary="Receive HubSpot webhook events")
async def hubspot_webhook(
    events: List[HubSpotWebhookEvent],
    hubspot_token: str = Query(..., description="HubSpot private app token"),
):
    """
    Receives deal.creation, deal.deletion, deal.propertyChange events.
    Updates local DB accordingly.
    """
    result = await service.handle_webhook(events, hubspot_token)
    return result
