"""
HubSpot service — full sync + real-time webhook handling.

Primary goals:
  1. Security  — no secret, no request processed.
  2. Deals     — all fields synced including dealtype, description.
  3. Associations — every create / update / delete reflected immediately in DB.
  4. Production-ready — soft deletes, idempotent upserts, chunked DB writes.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import AsyncGenerator, Dict, Iterable, List, Optional

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from app.db.models.hubspot import (
    Attachment,
    Company,
    Contact,
    Deal,
    DealCompany,
    DealContact,
    Job,
)
from app.db.session import SessionLocal
from app.modules.hubspot.hubspot_client import HubSpotClient
from app.modules.hubspot.load_orchestrator import LoadOrchestrator
from app.modules.hubspot.schema import HubSpotWebhookEvent, LoadProgress, LoadStatus

logger = logging.getLogger(__name__)

# In-memory job cache (lost on restart — DB is the source of truth)
_jobs: Dict[str, LoadProgress] = {}

# Properties that fire constantly but carry no meaningful DB change — skip them
IGNORED_PROPERTIES = {
    "hs_lastmodifieddate",
    "hs_last_activity_date",
    "hs_num_associated_contacts",
    "hs_activity_count",
    "lastmodifieddate",
    "notes_last_activity",
    "hs_updates_followed_contacts_count",
    "hs_time_in_",       # prefix — matched below
    "notes_next_activity_date",
    "engagements_last_meeting_booked",
}

# Deal properties to fetch on every deal read
DEAL_PROPERTIES = [
    "dealname",
    "amount",
    "dealstage",
    "closedate",
    "pipeline",
    "hubspot_owner_id",
    "deal_owner_email",
    "delivery_owner",
    "project_start_date",
    "project_end_date",
    "po_hours",
    "dealtype",
    "description",
    "createdate",
    "hs_lastmodifieddate",
    "hs_object_id",
]

CONTACT_PROPERTIES = [
    "email",
    "firstname",
    "lastname",
    "phone",
    "mobilephone",
    "city",
    "country",
    "company",
    "closedate",
    "createdate",
    "lastmodifieddate",
    "hs_object_id",
]

COMPANY_PROPERTIES = [
    "name",
    "domain",
    "industry",
    "city",
    "country",
    "createdate",
    "hs_lastmodifieddate",
    "hs_object_id",
]


def _is_ignored_property(name: Optional[str]) -> bool:
    if not name:
        return False
    if name in IGNORED_PROPERTIES:
        return True
    if name.startswith("hs_time_in_"):
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# FULL LOAD ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

async def start_load(
    hubspot_token: str,
    deal_properties: str,
    contact_properties: str,
    company_properties: str,
) -> StreamingResponse:
    """
    Full HubSpot sync with live SSE streaming progress.
    Azure Functions safe — no BackgroundTasks required.
    """
    job_id = f"load_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"

    db = SessionLocal()
    try:
        create_job(db, job_id=job_id, started_at=datetime.utcnow())
    finally:
        db.close()

    progress = LoadProgress(job_id=job_id, status=LoadStatus.RUNNING)
    _jobs[job_id] = progress

    client = HubSpotClient(access_token=hubspot_token)
    orchestrator = LoadOrchestrator(client=client)

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            async for update_event in orchestrator.run(
                deal_properties=deal_properties.split(","),
                contact_properties=contact_properties.split(","),
                company_properties=company_properties.split(","),
            ):
                progress.deals_fetched = update_event.deals_fetched
                progress.contacts_fetched = update_event.contacts_fetched
                progress.companies_fetched = update_event.companies_fetched
                progress.attachments_fetched = update_event.attachments_fetched
                progress.pages_processed = update_event.pages_processed
                progress.api_calls_made = update_event.api_calls_made
                progress.errors = update_event.errors
                progress.result_sample = update_event.result_sample

                db = SessionLocal()
                try:
                    update_job_from_progress(db, job_id=job_id, progress=progress)
                finally:
                    db.close()

                yield f"data: {progress.model_dump_json()}\n\n"
                logger.info(
                    "[%s] Progress: %d deals, %d API calls",
                    job_id,
                    progress.deals_fetched,
                    progress.api_calls_made,
                )

            # ── Completed ─────────────────────────────────────────────────────
            progress.status = LoadStatus.COMPLETED
            progress.completed_at = datetime.utcnow().isoformat()

            db = SessionLocal()
            try:
                update_job_from_progress(db, job_id=job_id, progress=progress)
                result_data = orchestrator.get_result_data()
                if result_data:
                    persist_load_data(db, **result_data)
            finally:
                db.close()

            yield f"data: {progress.model_dump_json()}\n\n"
            logger.info("[%s] COMPLETED — %d deals", job_id, progress.deals_fetched)

        except Exception as exc:
            logger.exception("[%s] FAILED", job_id)
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
            _jobs.pop(job_id, None)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        },
    )


# ═══════════════════════════════════════════════════════════════════════════════
# WEBHOOK HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

async def handle_webhook(events: List[HubSpotWebhookEvent], hubspot_token: str) -> Dict:
    """
    Process all incoming HubSpot webhook events.

    Two optimisations applied before the main loop:

    1. DEDUP — When one user action fires N property/creation events for the
       same object (e.g. company creation fires 5 events), we only need one
       API fetch+upsert.  We keep the LAST event per (object_type, objectId)
       so we always act on the most recent state.

    2. MIRROR SKIP — Association events always arrive in both directions
       (DEAL_TO_CONTACT + CONTACT_TO_DEAL).  We only process the DEAL_TO_*
       direction; the mirror is a no-op because the DB row is keyed on
       (deal_id, contact/company_id).
    """
    processed = 0
    errors: List[str] = []

    # ── FIX 1: deduplicate property/creation events per object within batch ──
    # Keep only the last event per (object_type, objectId).
    # Deletion and association events are never deduplicated.
    seen_upsert_keys: set = set()
    deduped: List[HubSpotWebhookEvent] = []

    for event in reversed(events):
        sub = event.subscriptionType or ""
        is_upsert = sub.endswith(".creation") or sub.endswith(".propertyChange")
        if is_upsert and event.objectId is not None:
            key = (sub.split(".")[0], event.objectId)   # e.g. ("company", 315089583808)
            if key in seen_upsert_keys:
                processed += 1   # count as processed — just skipped
                continue
            seen_upsert_keys.add(key)
        deduped.append(event)

    deduped.reverse()   # restore original chronological order

    for event in deduped:
        try:
            # objectId is None on associationChange events — use empty string
            # so existing log lines and soft-delete helpers still work safely.
            object_id = str(event.objectId) if event.objectId is not None else ""
            sub_type = event.subscriptionType

            # Skip noisy system-property events
            if sub_type.endswith(".propertyChange") and _is_ignored_property(event.propertyName):
                logger.debug("Skipping ignored property event: %s", event.propertyName)
                processed += 1
                continue

            # ── DEAL ──────────────────────────────────────────────────────────
            if sub_type == "deal.deletion":
                db = SessionLocal()
                try:
                    _soft_delete_deal(db, object_id)
                finally:
                    db.close()

            elif sub_type in ("deal.creation", "deal.propertyChange"):
                client = HubSpotClient(access_token=hubspot_token)
                try:
                    deal_data = await _fetch_single_deal(client, object_id)
                    if deal_data:
                        stage_map = await _get_stage_map(client)
                        db = SessionLocal()
                        try:
                            _upsert_deals(db, [deal_data], stage_map, change_source="webhook")
                            db.commit()
                            logger.info("Upserted deal %s via webhook (%s)", object_id, sub_type)
                        finally:
                            db.close()
                finally:
                    await client.close()

            elif sub_type == "deal.associationChange":
                db = SessionLocal()
                try:
                    _handle_association_change(db, event, source="webhook")
                    db.commit()
                finally:
                    db.close()

            # ── CONTACT ───────────────────────────────────────────────────────
            elif sub_type == "contact.deletion":
                db = SessionLocal()
                try:
                    _soft_delete_contact(db, object_id)
                finally:
                    db.close()

            elif sub_type in ("contact.creation", "contact.propertyChange"):
                client = HubSpotClient(access_token=hubspot_token)
                try:
                    contact_data = await _fetch_single_contact(client, object_id)
                    if contact_data:
                        props = contact_data.get("properties", {})
                        db = SessionLocal()
                        try:
                            _upsert_contacts(db, {object_id: props}, change_source="webhook")
                            db.commit()
                            logger.info("Upserted contact %s via webhook (%s)", object_id, sub_type)
                        finally:
                            db.close()
                finally:
                    await client.close()

            elif sub_type == "contact.associationChange":
                db = SessionLocal()
                try:
                    _handle_association_change(db, event, source="webhook")
                    db.commit()
                finally:
                    db.close()

            # ── COMPANY ───────────────────────────────────────────────────────
            elif sub_type == "company.deletion":
                db = SessionLocal()
                try:
                    _soft_delete_company(db, object_id)
                finally:
                    db.close()

            elif sub_type in ("company.creation", "company.propertyChange"):
                client = HubSpotClient(access_token=hubspot_token)
                try:
                    company_data = await _fetch_single_company(client, object_id)
                    if company_data:
                        props = company_data.get("properties", {})
                        db = SessionLocal()
                        try:
                            _upsert_companies(db, {object_id: props}, change_source="webhook")
                            db.commit()
                            logger.info("Upserted company %s via webhook (%s)", object_id, sub_type)
                        finally:
                            db.close()
                finally:
                    await client.close()

            elif sub_type == "company.associationChange":
                db = SessionLocal()
                try:
                    _handle_association_change(db, event, source="webhook")
                    db.commit()
                finally:
                    db.close()

            else:
                logger.warning("Unhandled subscriptionType: %s", sub_type)

            processed += 1
            logger.info("Webhook processed: %s for objectId=%s", sub_type, object_id)

        except Exception as exc:
            logger.error("Webhook error for eventId=%s: %s", event.eventId, exc, exc_info=True)
            errors.append(f"eventId={event.eventId} type={event.subscriptionType}: {exc}")

    return {"processed": processed, "errors": errors}


# ═══════════════════════════════════════════════════════════════════════════════
# ASSOCIATION CHANGE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

def _handle_association_change(
    db: Session,
    event: HubSpotWebhookEvent,
    source: str = "webhook",
) -> None:
    """
    Handle a HubSpot associationChange event.

    HubSpot always fires BOTH directions for every association action:
        DEAL_TO_CONTACT  +  CONTACT_TO_DEAL
        DEAL_TO_COMPANY  +  COMPANY_TO_DEAL

    FIX 2: We skip the mirror directions (CONTACT_TO_DEAL, COMPANY_TO_DEAL)
    entirely.  The DB row is keyed on (deal_id, contact/company_id) and the
    DEAL_TO_* event already carries the correct IDs, so processing the mirror
    would just write the same row a second time.
    """
    a_type = event.associationType
    if not a_type:
        logger.warning(
            "associationChange event %s has no associationType — skipping",
            event.eventId,
        )
        return

    # ── FIX 2: skip mirror-direction events ───────────────────────────────────
    if a_type in ("CONTACT_TO_DEAL", "COMPANY_TO_DEAL"):
        logger.debug(
            "Skipping mirror association event %s (type=%s) — handled by DEAL_TO_ counterpart",
            event.eventId, a_type,
        )
        return

    removed = bool(event.associationRemoved)
    is_primary = bool(event.isPrimaryAssociation)
    from_id = str(event.fromObjectId) if event.fromObjectId else None
    to_id = str(event.toObjectId) if event.toObjectId else None
    now = datetime.utcnow()

    # ── DEAL ↔ CONTACT ────────────────────────────────────────────────────────
    if a_type == "DEAL_TO_CONTACT":
        deal_id, contact_id = from_id, to_id

        if not deal_id or not contact_id:
            logger.warning("associationChange %s missing IDs — skipping", event.eventId)
            return

        if removed:
            db.execute(
                update(DealContact)
                .where(DealContact.deal_id == deal_id, DealContact.contact_id == contact_id)
                .values(deleted_at=now, updated_at=now, change_source=source)
            )
            logger.info("ASSOC REMOVED: deal %s ↔ contact %s", deal_id, contact_id)
        else:
            existing = db.execute(
                select(DealContact).where(
                    DealContact.deal_id == deal_id,
                    DealContact.contact_id == contact_id,
                )
            ).scalar_one_or_none()

            if existing:
                existing.deleted_at = None
                existing.updated_at = now
                existing.is_primary = is_primary
                existing.association_type = a_type
                existing.change_source = source
            else:
                db.add(DealContact(
                    deal_id=deal_id,
                    contact_id=contact_id,
                    association_type=a_type,
                    is_primary=is_primary,
                    change_source=source,
                    created_at=now,
                    updated_at=now,
                    deleted_at=None,
                ))
            logger.info(
                "ASSOC ADDED/RESTORED: deal %s ↔ contact %s (primary=%s)",
                deal_id, contact_id, is_primary,
            )

    # ── DEAL ↔ COMPANY ────────────────────────────────────────────────────────
    elif a_type == "DEAL_TO_COMPANY":
        deal_id, company_id = from_id, to_id

        if not deal_id or not company_id:
            logger.warning("associationChange %s missing IDs — skipping", event.eventId)
            return

        if removed:
            db.execute(
                update(DealCompany)
                .where(DealCompany.deal_id == deal_id, DealCompany.company_id == company_id)
                .values(deleted_at=now, updated_at=now, change_source=source)
            )
            logger.info("ASSOC REMOVED: deal %s ↔ company %s", deal_id, company_id)
        else:
            existing = db.execute(
                select(DealCompany).where(
                    DealCompany.deal_id == deal_id,
                    DealCompany.company_id == company_id,
                )
            ).scalar_one_or_none()

            if existing:
                existing.deleted_at = None
                existing.updated_at = now
                existing.is_primary = is_primary
                existing.association_type = a_type
                existing.change_source = source
            else:
                db.add(DealCompany(
                    deal_id=deal_id,
                    company_id=company_id,
                    association_type=a_type,
                    is_primary=is_primary,
                    change_source=source,
                    created_at=now,
                    updated_at=now,
                    deleted_at=None,
                ))
            logger.info(
                "ASSOC ADDED/RESTORED: deal %s ↔ company %s (primary=%s)",
                deal_id, company_id, is_primary,
            )

    else:
        logger.debug("Unhandled association type: %s — ignoring", a_type)


# ═══════════════════════════════════════════════════════════════════════════════
# HUBSPOT API FETCH HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

async def _fetch_single_deal(client: HubSpotClient, deal_id: str) -> Optional[Dict]:
    try:
        data = await client.batch_read_objects("deals", [deal_id], DEAL_PROPERTIES)
        results = data.get("results", [])
        return results[0] if results else None
    except Exception as exc:
        logger.error("Failed to fetch deal %s: %s", deal_id, exc)
        return None


async def _fetch_single_contact(client: HubSpotClient, contact_id: str) -> Optional[Dict]:
    try:
        data = await client.batch_read_objects("contacts", [contact_id], CONTACT_PROPERTIES)
        results = data.get("results", [])
        return results[0] if results else None
    except Exception as exc:
        logger.error("Failed to fetch contact %s: %s", contact_id, exc)
        return None


async def _fetch_single_company(client: HubSpotClient, company_id: str) -> Optional[Dict]:
    try:
        data = await client.batch_read_objects("companies", [company_id], COMPANY_PROPERTIES)
        results = data.get("results", [])
        return results[0] if results else None
    except Exception as exc:
        logger.error("Failed to fetch company %s: %s", company_id, exc)
        return None


async def _get_stage_map(client: HubSpotClient) -> Dict[str, str]:
    try:
        data = await client.get_pipeline_stages("79964941")
        return {stage["id"]: stage["label"] for stage in data.get("results", [])}
    except Exception as exc:
        logger.error("Failed to fetch stage map: %s", exc)
        return {}


async def _sync_deal_associations(client: HubSpotClient, deal_id: str, source: str = "webhook") -> None:
    """Re-sync all contact + company associations for a deal from HubSpot API."""
    try:
        contact_data, company_data = await asyncio.gather(
            client.batch_get_associations("deals", "contacts", [deal_id]),
            client.batch_get_associations("deals", "companies", [deal_id]),
        )

        contact_ids = [
            r["id"]
            for result in contact_data.get("results", [])
            for r in result.get("to", [])
        ]
        company_ids = [
            r["id"]
            for result in company_data.get("results", [])
            for r in result.get("to", [])
        ]

        now = datetime.utcnow()
        db = SessionLocal()
        try:
            db.execute(
                update(DealContact)
                .where(DealContact.deal_id == deal_id)
                .values(deleted_at=now, updated_at=now, change_source=source)
            )
            db.execute(
                update(DealCompany)
                .where(DealCompany.deal_id == deal_id)
                .values(deleted_at=now, updated_at=now, change_source=source)
            )

            for cid in contact_ids:
                existing = db.execute(
                    select(DealContact).where(
                        DealContact.deal_id == deal_id, DealContact.contact_id == cid
                    )
                ).scalar_one_or_none()
                if existing:
                    existing.deleted_at = None
                    existing.updated_at = now
                    existing.change_source = source
                else:
                    db.add(DealContact(
                        deal_id=deal_id, contact_id=cid,
                        association_type="DEAL_TO_CONTACT",
                        change_source=source, created_at=now, updated_at=now,
                    ))

            for cid in company_ids:
                existing = db.execute(
                    select(DealCompany).where(
                        DealCompany.deal_id == deal_id, DealCompany.company_id == cid
                    )
                ).scalar_one_or_none()
                if existing:
                    existing.deleted_at = None
                    existing.updated_at = now
                    existing.change_source = source
                else:
                    db.add(DealCompany(
                        deal_id=deal_id, company_id=cid,
                        association_type="DEAL_TO_COMPANY",
                        change_source=source, created_at=now, updated_at=now,
                    ))

            db.commit()
            logger.info(
                "Deal %s associations re-synced: %d contacts, %d companies",
                deal_id, len(contact_ids), len(company_ids),
            )
        finally:
            db.close()
    except Exception as exc:
        logger.error("Failed to re-sync associations for deal %s: %s", deal_id, exc)


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE UPSERT HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _upsert_deals(
    db: Session,
    deals: Iterable[Dict],
    stage_map: Dict[str, str],
    change_source: str = "initial_load",
) -> None:
    now = datetime.utcnow()
    for deal in deals:
        props = deal.get("properties", {})
        deal_id = str(deal.get("id", ""))
        stage_id = _get_prop(props, "dealstage") or ""
        db.merge(
            Deal(
                id=deal_id,
                hs_object_id=_get_prop(props, "hs_object_id") or deal_id,
                dealname=_get_prop(props, "dealname"),
                amount=_get_prop(props, "amount"),
                dealstage=stage_id,
                dealstage_label=stage_map.get(stage_id),
                closedate=_get_prop(props, "closedate"),
                createdate=_get_prop(props, "createdate"),
                hs_lastmodifieddate=_get_prop(props, "hs_lastmodifieddate"),
                hubspot_owner_id=_get_prop(props, "hubspot_owner_id"),
                pipeline=_get_prop(props, "pipeline"),
                deal_owner_email=_get_prop(props, "deal_owner_email"),
                delivery_owner=_get_prop(props, "delivery_owner"),
                project_start_date=_get_prop(props, "project_start_date"),
                project_end_date=_get_prop(props, "project_end_date"),
                po_hours=_get_prop(props, "po_hours"),
                dealtype=_get_prop(props, "dealtype"),
                description=_get_prop(props, "description"),
                synced_at=now,
                deleted_at=None,
            )
        )


def _upsert_contacts(
    db: Session,
    contacts_map: Dict[str, Dict],
    change_source: str = "initial_load",
) -> None:
    now = datetime.utcnow()
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
                mobilephone=_get_prop(props, "mobilephone"),
                city=_get_prop(props, "city"),
                country=_get_prop(props, "country"),
                company=_get_prop(props, "company"),
                closedate=_get_prop(props, "closedate"),
                createdate=_get_prop(props, "createdate"),
                lastmodifieddate=_get_prop(props, "lastmodifieddate"),
                synced_at=now,
                deleted_at=None,
            )
        )


def _upsert_companies(
    db: Session,
    companies_map: Dict[str, Dict],
    change_source: str = "initial_load",
) -> None:
    now = datetime.utcnow()
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
                country=_get_prop(props, "country"),
                createdate=_get_prop(props, "createdate"),
                hs_lastmodifieddate=_get_prop(props, "hs_lastmodifieddate"),
                synced_at=now,
                deleted_at=None,
            )
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SOFT DELETE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _soft_delete_deal(db: Session, deal_id: str) -> None:
    now = datetime.utcnow()
    db.execute(update(Deal).where(Deal.id == deal_id).values(deleted_at=now, synced_at=now))
    db.execute(
        update(DealContact)
        .where(DealContact.deal_id == deal_id)
        .values(deleted_at=now, updated_at=now, change_source="webhook_deletion")
    )
    db.execute(
        update(DealCompany)
        .where(DealCompany.deal_id == deal_id)
        .values(deleted_at=now, updated_at=now, change_source="webhook_deletion")
    )
    db.commit()
    logger.info("Soft-deleted deal %s and all its associations", deal_id)


def _soft_delete_contact(db: Session, contact_id: str) -> None:
    now = datetime.utcnow()
    db.execute(update(Contact).where(Contact.id == contact_id).values(deleted_at=now, synced_at=now))
    db.execute(
        update(DealContact)
        .where(DealContact.contact_id == contact_id)
        .values(deleted_at=now, updated_at=now, change_source="webhook_deletion")
    )
    db.commit()
    logger.info("Soft-deleted contact %s and its deal associations", contact_id)


def _soft_delete_company(db: Session, company_id: str) -> None:
    now = datetime.utcnow()
    db.execute(update(Company).where(Company.id == company_id).values(deleted_at=now, synced_at=now))
    db.execute(
        update(DealCompany)
        .where(DealCompany.company_id == company_id)
        .values(deleted_at=now, updated_at=now, change_source="webhook_deletion")
    )
    db.commit()
    logger.info("Soft-deleted company %s and its deal associations", company_id)


# ═══════════════════════════════════════════════════════════════════════════════
# FULL LOAD PERSIST
# ═══════════════════════════════════════════════════════════════════════════════

def persist_load_data(
    db: Session,
    deals: List[Dict],
    contact_assoc: Dict[str, List[str]],
    company_assoc: Dict[str, List[str]],
    contacts_map: Dict[str, Dict],
    companies_map: Dict[str, Dict],
    attachments_map: Dict[str, List],
    stage_map: Dict[str, str],
) -> None:
    _upsert_deals(db, deals, stage_map, change_source="initial_load")
    _upsert_contacts(db, contacts_map, change_source="initial_load")
    _upsert_companies(db, companies_map, change_source="initial_load")
    _replace_deal_contacts(db, contact_assoc)
    _replace_deal_companies(db, company_assoc)
    _replace_attachments(db, attachments_map)
    db.commit()


def _replace_deal_contacts(db: Session, contact_assoc: Dict[str, List[str]]) -> None:
    deal_ids = list(contact_assoc.keys())
    now = datetime.utcnow()

    for chunk in _chunk_list(deal_ids, 500):
        db.execute(
            update(DealContact)
            .where(DealContact.deal_id.in_(chunk))
            .values(deleted_at=now, updated_at=now, change_source="initial_load_replace")
        )

    for deal_id, contact_ids in contact_assoc.items():
        for contact_id in contact_ids:
            existing = db.execute(
                select(DealContact).where(
                    DealContact.deal_id == str(deal_id),
                    DealContact.contact_id == str(contact_id),
                )
            ).scalar_one_or_none()
            if existing:
                existing.deleted_at = None
                existing.updated_at = now
                existing.change_source = "initial_load"
            else:
                db.add(DealContact(
                    deal_id=str(deal_id),
                    contact_id=str(contact_id),
                    association_type="DEAL_TO_CONTACT",
                    change_source="initial_load",
                    created_at=now,
                    updated_at=now,
                    deleted_at=None,
                ))


def _replace_deal_companies(db: Session, company_assoc: Dict[str, List[str]]) -> None:
    deal_ids = list(company_assoc.keys())
    now = datetime.utcnow()

    for chunk in _chunk_list(deal_ids, 500):
        db.execute(
            update(DealCompany)
            .where(DealCompany.deal_id.in_(chunk))
            .values(deleted_at=now, updated_at=now, change_source="initial_load_replace")
        )

    for deal_id, company_ids in company_assoc.items():
        for company_id in company_ids:
            existing = db.execute(
                select(DealCompany).where(
                    DealCompany.deal_id == str(deal_id),
                    DealCompany.company_id == str(company_id),
                )
            ).scalar_one_or_none()
            if existing:
                existing.deleted_at = None
                existing.updated_at = now
                existing.change_source = "initial_load"
            else:
                db.add(DealCompany(
                    deal_id=str(deal_id),
                    company_id=str(company_id),
                    association_type="DEAL_TO_COMPANY",
                    change_source="initial_load",
                    created_at=now,
                    updated_at=now,
                    deleted_at=None,
                ))


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


# ═══════════════════════════════════════════════════════════════════════════════
# JOB DB HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def create_job(db: Session, job_id: str, started_at: datetime) -> Job:
    job = Job(job_id=job_id, status=LoadStatus.RUNNING.value, started_at=started_at)
    db.add(job)
    db.commit()
    return job


def update_job_from_progress(db: Session, job_id: str, progress: LoadProgress) -> Job:
    job = db.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none()
    if not job:
        job = Job(job_id=job_id, started_at=datetime.utcnow())
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


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _chunk_list(items: List, size: int) -> List[List]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _get_prop(props: Dict, key: str) -> Optional[str]:
    value = props.get(key)
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None