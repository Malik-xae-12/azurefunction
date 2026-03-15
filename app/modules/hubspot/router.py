"""HubSpot module routes."""

from fastapi import APIRouter, BackgroundTasks

from app.modules.hubspot import service
from app.modules.hubspot.schema import LoadProgress

router = APIRouter()


@router.post("/load/start", summary="Trigger full initial load")
async def start_load(
    background_tasks: BackgroundTasks,
    hubspot_token: str,
    deal_properties: str = "dealname,amount,dealstage,closedate,pipeline,hubspot_owner_id",
    contact_properties: str = "firstname,lastname,email,phone",
    company_properties: str = "name,domain,industry,city",
):
    return await service.start_load(
        background_tasks=background_tasks,
        hubspot_token=hubspot_token,
        deal_properties=deal_properties,
        contact_properties=contact_properties,
        company_properties=company_properties,
    )


@router.get("/load/status/{job_id}", response_model=LoadProgress, summary="Poll job progress")
async def get_status(job_id: str):
    return await service.get_status(job_id)


@router.get("/load/stream/{job_id}", summary="SSE stream of real-time progress")
async def stream_progress(job_id: str):
    return await service.stream_progress(job_id)


@router.get("/load/result/{job_id}", summary="Get enriched result after completion")
async def get_result(job_id: str):
    return await service.get_result(job_id)


@router.get("/", summary="Health check")
async def root():
    return {"status": "ok", "service": "HubSpot Loader", "docs": "/docs"}
