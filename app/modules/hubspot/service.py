"""HubSpot load service logic - AZURE FUNCTIONS READY."""

import asyncio
import json
import logging
from datetime import datetime
from typing import AsyncGenerator, Dict, Iterable, List, Optional

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.db.models.hubspot.associations import DealCompany, DealContact
from app.db.models.hubspot.attachment import Attachment
from app.db.models.hubspot.company import Company
from app.db.models.hubspot.contact import Contact
from app.db.models.hubspot.deal import Deal
from app.db.models.hubspot.job import Job
from app.db.session import SessionLocal
from app.modules.hubspot.hubspot_client import HubSpotClient
from app.modules.hubspot.load_orchestrator import LoadOrchestrator
from app.modules.hubspot.schema import LoadProgress, LoadStatus

logger = logging.getLogger(__name__)

_jobs: Dict[str, LoadProgress] = {}

async def start_load(
    hubspot_token: str,
    deal_properties: str,
    contact_properties: str,
    company_properties: str,
) -> StreamingResponse:
    """
    🚀 MAIN ENTRY POINT - Runs FULL HubSpot sync with LIVE streaming progress
    NO BackgroundTasks - Works perfectly in Azure Functions!
    """
    job_id = f"load_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
    
    # 1. Create DB job record FIRST
    db = SessionLocal()
    try:
        create_job(db, job_id=job_id, started_at=datetime.utcnow())
    finally:
        db.close()
    
    # 2. Initialize progress tracking
    progress = LoadProgress(job_id=job_id, status=LoadStatus.RUNNING)
    _jobs[job_id] = progress
    
    # 3. Create HubSpot orchestrator
    client = HubSpotClient(access_token=hubspot_token)
    orchestrator = LoadOrchestrator(client=client)
    
    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            # 🔥 STREAM LIVE PROGRESS - Phase 1→4 updates
            async for update in orchestrator.run(
                deal_properties=deal_properties.split(","),
                contact_properties=contact_properties.split(","),
                company_properties=company_properties.split(","),
            ):
                # Update progress object
                progress.deals_fetched = update.deals_fetched
                progress.contacts_fetched = update.contacts_fetched
                progress.companies_fetched = update.companies_fetched
                progress.attachments_fetched = update.attachments_fetched
                progress.pages_processed = update.pages_processed
                progress.api_calls_made = update.api_calls_made
                progress.errors = update.errors
                progress.result_sample = update.result_sample
                
                # Persist to database
                db = SessionLocal()
                try:
                    update_job_from_progress(db, job_id=job_id, progress=progress)
                finally:
                    db.close()
                
                # STREAM to browser/Postman
                yield f"data: {progress.model_dump_json()}\n\n"
                logger.info(f"[{job_id}] Progress: {progress.deals_fetched} deals, {progress.api_calls_made} API calls")
            
            # Mark as COMPLETED
            progress.status = LoadStatus.COMPLETED
            progress.completed_at = datetime.utcnow().isoformat()
            
            # Final save + persist ALL data
            db = SessionLocal()
            try:
                update_job_from_progress(db, job_id=job_id, progress=progress)
                result_data = orchestrator.get_result_data()
                if result_data:
                    persist_load_data(db, **result_data)
            finally:
                db.close()
            
            yield f"data: {progress.model_dump_json()}\n\n"
            logger.info(f"[{job_id}] ✅ COMPLETED - {progress.deals_fetched} deals processed")
            
        except Exception as exc:
            logger.exception(f"[{job_id}] ❌ FAILED")
            progress.status = LoadStatus.FAILED
            progress.error = str(exc)
            progress.completed_at = datetime.utcnow().isoformat()
            
            db = SessionLocal()
            try:
                update_job_from_progress(db, job_id=job_id, progress=progress)
            finally:
                db.close()
            
            yield f"data: {progress.model_dump_json()}\n\n"
        finally:
            # Cleanup
            if job_id in _jobs:
                del _jobs[job_id]
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        }
    )

async def get_status(job_id: str) -> LoadProgress:
    """📊 Check job status - memory cache → DB fallback"""
    if job_id in _jobs:
        return _jobs[job_id]
    
    db = SessionLocal()
    try:
        db_prog = get_job_progress(db, job_id)
        if not db_prog:
            raise HTTPException(status_code=404, detail="Job not found")
        return db_prog
    finally:
        db.close()

async def get_result(job_id: str) -> LoadProgress:
    """✅ Get completed job result"""
    progress = await get_status(job_id)
    if progress.status == LoadStatus.RUNNING:
        raise HTTPException(status_code=202, detail="Job still running - use /load/status")
    if progress.status == LoadStatus.FAILED:
        raise HTTPException(status_code=500, detail=progress.error)
    return progress

# ═══════════════════════════════════════════════════════════════════════════════
# ALL YOUR EXISTING DATABASE FUNCTIONS - UNCHANGED
# ═══════════════════════════════════════════════════════════════════════════════

def create_job(db: Session, job_id: str, started_at: datetime) -> Job:
    job = Job(job_id=job_id, status=LoadStatus.RUNNING.value, started_at=started_at)
    db.add(job)
    db.commit()
    return job

def update_job_from_progress(db: Session, job_id: str, progress: LoadProgress) -> Job:
    job = db.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none()
    if not job:
        job = Job(job_id=job_id, status=progress.status.value, started_at=datetime.utcnow())
        db.add(job)

    job.status = progress.status.value
    job.pages_processed = progress.pages_processed
    job.deals_fetched = progress.deals_fetched
    job.contacts_fetched = progress.contacts_fetched
    job.companies_fetched = progress.companies_fetched
    job.attachments_fetched = progress.attachments_fetched
    job.api_calls_made = progress.api_calls_made
    job.error = progress.error
    job.errors_json = json.dumps(progress.errors)
    job.result_sample_json = json.dumps(progress.result_sample)

    if progress.completed_at:
        job.completed_at = datetime.fromisoformat(progress.completed_at)

    db.commit()
    return job

def get_job_progress(db: Session, job_id: str) -> Optional[LoadProgress]:
    job = db.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none()
    if not job:
        return None

    errors = json.loads(job.errors_json) if job.errors_json else []
    result_sample = json.loads(job.result_sample_json) if job.result_sample_json else []

    return LoadProgress(
        job_id=job.job_id,
        status=LoadStatus(job.status),
        pages_processed=job.pages_processed,
        deals_fetched=job.deals_fetched,
        contacts_fetched=job.contacts_fetched,
        companies_fetched=job.companies_fetched,
        attachments_fetched=job.attachments_fetched,
        api_calls_made=job.api_calls_made,
        errors=errors,
        error=job.error,
        started_at=job.started_at.isoformat(),
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
        result_sample=result_sample,
    )

def persist_load_data(
    db: Session,
    deals: List[Dict],
    contact_assoc: Dict[str, List[str]],
    company_assoc: Dict[str, List[str]],
    contacts_map: Dict[str, Dict],
    companies_map: Dict[str, Dict],
    attachments_map: Dict[str, List],
) -> None:
    _upsert_deals(db, deals)
    _upsert_contacts(db, contacts_map)
    _upsert_companies(db, companies_map)
    _replace_deal_contacts(db, contact_assoc)
    _replace_deal_companies(db, company_assoc)
    _replace_attachments(db, attachments_map)
    db.commit()

def _upsert_deals(db: Session, deals: Iterable[Dict]) -> None:
    for deal in deals:
        props = deal.get("properties", {})
        deal_id = str(deal.get("id", ""))
        db.merge(
            Deal(
                id=deal_id,
                hs_object_id=_get_prop(props, "hs_object_id") or deal_id,
                dealname=_get_prop(props, "dealname"),
                amount=_get_prop(props, "amount"),
                dealstage=_get_prop(props, "dealstage"),
                closedate=_get_prop(props, "closedate"),
                createdate=_get_prop(props, "createdate"),
                hs_lastmodifieddate=_get_prop(props, "hs_lastmodifieddate"),
                hubspot_owner_id=_get_prop(props, "hubspot_owner_id"),
                pipeline=_get_prop(props, "pipeline"),
            )
        )

def _upsert_contacts(db: Session, contacts_map: Dict[str, Dict]) -> None:
    for contact_id, props in contacts_map.items():
        cid = str(contact_id)
        db.merge(
            Contact(
                id=cid,
                hs_object_id=_get_prop(props, "hs_object_id") or cid,
                email=_get_prop(props, "email"),
                firstname=_get_prop(props, "firstname"),
                lastname=_get_prop(props, "lastname"),
                phone=_get_prop(props, "phone"),
                createdate=_get_prop(props, "createdate"),
                lastmodifieddate=_get_prop(props, "lastmodifieddate"),
            )
        )

def _upsert_companies(db: Session, companies_map: Dict[str, Dict]) -> None:
    for company_id, props in companies_map.items():
        cid = str(company_id)
        db.merge(
            Company(
                id=cid,
                hs_object_id=_get_prop(props, "hs_object_id") or cid,
                name=_get_prop(props, "name"),
                domain=_get_prop(props, "domain"),
                industry=_get_prop(props, "industry"),
                city=_get_prop(props, "city"),
                createdate=_get_prop(props, "createdate"),
                hs_lastmodifieddate=_get_prop(props, "hs_lastmodifieddate"),
            )
        )

def _replace_deal_contacts(db: Session, contact_assoc: Dict[str, List[str]]) -> None:
    deal_ids = list(contact_assoc.keys())
    for chunk in _chunk_list(deal_ids, 500):
        db.execute(delete(DealContact).where(DealContact.deal_id.in_(chunk)))

    rows = []
    for deal_id, contact_ids in contact_assoc.items():
        for contact_id in contact_ids:
            rows.append(DealContact(deal_id=str(deal_id), contact_id=str(contact_id)))
    db.add_all(rows)

def _replace_deal_companies(db: Session, company_assoc: Dict[str, List[str]]) -> None:
    deal_ids = list(company_assoc.keys())
    for chunk in _chunk_list(deal_ids, 500):
        db.execute(delete(DealCompany).where(DealCompany.deal_id.in_(chunk)))

    rows = []
    for deal_id, company_ids in company_assoc.items():
        for company_id in company_ids:
            rows.append(DealCompany(deal_id=str(deal_id), company_id=str(company_id)))
    db.add_all(rows)

def _replace_attachments(db: Session, attachments_map: Dict[str, List]) -> None:
    deal_ids = list(attachments_map.keys())
    for chunk in _chunk_list(deal_ids, 500):
        db.execute(delete(Attachment).where(Attachment.deal_id.in_(chunk)))

    rows = []
    for deal_id, attachments in attachments_map.items():
        for attachment in attachments:
            rows.append(
                Attachment(
                    deal_id=str(attachment.get("deal_id", deal_id)),
                    note_id=str(attachment.get("note_id", "")),
                    file_id=str(attachment.get("file_id", "")),
                )
            )
    db.add_all(rows)

def _chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]

def _get_prop(props: Dict, key: str) -> Optional[str]:
    value = props.get(key)
    if value is None:
        return None
    return str(value)
