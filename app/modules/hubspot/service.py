"""
HubSpot service layer.

Webhook events handled:
  deal.creation / deal.propertyChange / deal.deletion / deal.associationChange
  contact.propertyChange   → if contact exists in DB: update it + write "contact updated" audit
  company.propertyChange   → if company exists in DB: update it + write "company updated" audit

Audit actions written (human-readable):
  deal created, deal updated, deal deleted
  contact created, contact updated, contact linked, contact removed
  company created, company updated, company linked, company removed

Contact/Company have NO soft-delete. Records are simply upserted or deleted in place.
Association tables (deal_contacts, deal_companies) carry the link lifecycle via deleted_at.

performed_by resolution from webhook sourceId:
  "userId:89164629"           → performed_by = "89164629"
  "PRIMARY_AUTOMATION" / None → performed_by = None  (system)
  "objectTypeId:0-1;..."      → performed_by = None  (cascade)
"""

import json
import logging
from typing import AsyncGenerator, Dict, Iterable, List, Optional

from fastapi.responses import StreamingResponse
from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from app.db.models.hubspot import (
    Attachment,
    AuditLog,
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
from app.utils.timezone import now_ist

logger = logging.getLogger(__name__)

_jobs: Dict[str, LoadProgress] = {}

# ── Properties to skip entirely (noise / computed by HubSpot) ────────────────
IGNORED_PROPERTIES = {
    "hs_lastmodifieddate",
    "hs_last_activity_date",
    "hs_num_associated_contacts",
    "hs_activity_count",
    "lastmodifieddate",
    "notes_last_activity",
    "hs_updates_followed_contacts_count",
    "notes_next_activity_date",
    "engagements_last_meeting_booked",
}

# ── HubSpot API property lists ────────────────────────────────────────────────
DEAL_PROPERTIES = [
    "dealname", "amount", "dealstage", "closedate", "pipeline",
    "hubspot_owner_id", "deal_owner_email", "delivery_owner",
    "project_start_date", "project_end_date", "po_hours",
    "dealtype", "description", "createdate", "hs_lastmodifieddate", "hs_object_id",
]
CONTACT_PROPERTIES = [
    "email", "firstname", "lastname", "hubspot_owner_id",
    "jobtitle", "phone", "lifecyclestage", "hs_lead_status",
    "createdate", "lastmodifieddate", "hs_object_id",
]
COMPANY_PROPERTIES = [
    "domain", "name", "hubspot_owner_id", "industry", "type",
    "city", "state", "zip", "numberofemployees", "annualrevenue",
    "timezone", "description", "linkedin_company_page",
    "createdate", "hs_lastmodifieddate", "hs_object_id",
]

# ── Fields tracked in audit diffs ────────────────────────────────────────────
_DEAL_AUDIT_FIELDS = {
    "dealname", "amount", "dealstage", "dealstage_label", "closedate",
    "pipeline", "dealtype", "description", "hubspot_owner_id",
    "deal_owner_email", "delivery_owner", "project_start_date",
    "project_end_date", "po_hours",
}
_CONTACT_AUDIT_FIELDS = {
    "email", "firstname", "lastname", "hubspot_owner_id",
    "jobtitle", "phone", "lifecyclestage", "lead_status",
}
_COMPANY_AUDIT_FIELDS = {
    "domain", "name", "hubspot_owner_id", "industry", "company_type",
    "city", "state", "postal_code", "number_of_employees",
    "annual_revenue", "timezone", "description", "linkedin_company_page",
}


def _is_ignored_property(name: Optional[str]) -> bool:
    if not name:
        return False
    return name in IGNORED_PROPERTIES or name.startswith("hs_time_in_")


# ═══════════════════════════════════════════════════════════════════════════════
# WHO DID IT — resolve from webhook sourceId
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_actor(event: HubSpotWebhookEvent) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (performed_by, performed_by_raw).

    performed_by     : HubSpot userId string e.g. "89164629", or None for system events
    performed_by_raw : full raw sourceId string for traceability

    sourceId formats:
      "userId:89164629"                                → human user
      "objectTypeId:0-1;originalObjectId:460880..."    → system cascade
      None / absent                                    → automation or initial_load
    """
    source_id: Optional[str] = getattr(event, "sourceId", None)
    if not source_id:
        return None, None
    if source_id.startswith("userId:"):
        return source_id.split("userId:", 1)[1].strip(), source_id
    return None, source_id


# ═══════════════════════════════════════════════════════════════════════════════
# AUDIT HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def _write_audit(
    db: Session,
    *,
    table_name: str,
    record_id: str,
    action: str,                          # human-readable e.g. "deal updated"
    changed_fields: Optional[Dict] = None,
    deal_id: Optional[str] = None,
    related_table: Optional[str] = None,
    related_id: Optional[str] = None,
    performed_by: Optional[str] = None,
    performed_by_raw: Optional[str] = None,
    source: Optional[str] = None,
    hs_event_id: Optional[str] = None,
    hs_occurred_at: Optional[str] = None,
) -> None:
    db.add(AuditLog(
        table_name=table_name,
        record_id=str(record_id),
        action=action,
        changed_fields=json.dumps(changed_fields) if changed_fields else None,
        deal_id=deal_id,
        related_table=related_table,
        related_id=related_id,
        performed_by=performed_by,
        performed_by_raw=performed_by_raw,
        source=source,
        hs_event_id=str(hs_event_id) if hs_event_id is not None else None,
        hs_occurred_at=str(hs_occurred_at) if hs_occurred_at is not None else None,
        created_at=now_ist(),
    ))


def _diff_fields(old_obj, new_values: Dict, tracked: set) -> Dict:
    def _n(v):
        if v is None:
            return None
        s = str(v).strip()
        return s or None

    return {
        f: {"old": _n(getattr(old_obj, f, None)), "new": _n(new_values.get(f))}
        for f in tracked
        if _n(getattr(old_obj, f, None)) != _n(new_values.get(f))
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FULL LOAD ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

async def start_load(
    hubspot_token: str,
    deal_properties: str,
    contact_properties: str,
    company_properties: str,
) -> StreamingResponse:
    job_id = f"load_{now_ist().strftime('%Y%m%d_%H%M%S_%f')}"

    db = SessionLocal()
    try:
        create_job(db, job_id=job_id, started_at=now_ist())
    finally:
        db.close()

    progress = LoadProgress(job_id=job_id, status=LoadStatus.RUNNING)
    _jobs[job_id] = progress

    client = HubSpotClient(access_token=hubspot_token)
    orchestrator = LoadOrchestrator(client=client)

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            async for upd in orchestrator.run(
                deal_properties=deal_properties.split(","),
                contact_properties=contact_properties.split(","),
                company_properties=company_properties.split(","),
            ):
                progress.deals_fetched = upd.deals_fetched
                progress.contacts_fetched = upd.contacts_fetched
                progress.companies_fetched = upd.companies_fetched
                progress.attachments_fetched = upd.attachments_fetched
                progress.pages_processed = upd.pages_processed
                progress.api_calls_made = upd.api_calls_made
                progress.errors = upd.errors
                progress.result_sample = upd.result_sample

                db = SessionLocal()
                try:
                    update_job_from_progress(db, job_id=job_id, progress=progress)
                finally:
                    db.close()

                yield f"data: {progress.model_dump_json()}\n\n"

            progress.status = LoadStatus.COMPLETED
            progress.completed_at = now_ist().isoformat()

            db = SessionLocal()
            try:
                update_job_from_progress(db, job_id=job_id, progress=progress)
                result_data = orchestrator.get_result_data()
                if result_data:
                    persist_load_data(db, **result_data)
            finally:
                db.close()

            yield f"data: {progress.model_dump_json()}\n\n"

        except Exception as exc:
            logger.exception("[%s] FAILED", job_id)
            progress.status = LoadStatus.FAILED
            progress.error = str(exc)
            progress.completed_at = now_ist().isoformat()

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
# WEBHOOK DISPATCHER
# ═══════════════════════════════════════════════════════════════════════════════

async def handle_webhook(events: List[HubSpotWebhookEvent], hubspot_token: str) -> Dict:
    """
    Dispatch HubSpot webhook events.

    contact.propertyChange / company.propertyChange:
      → check if record exists in DB
      → if yes: fetch full record from HubSpot API, update DB, write audit
      → if no:  skip (we only track contacts/companies that are linked to deals)

    deal.associationChange:
      → adds "contact linked" / "company linked" or "contact removed" / "company removed" to audit
    """
    processed = 0
    errors: List[str] = []

    # Dedup: for creation/propertyChange keep only the last event per (type, objectId)
    seen: set = set()
    deduped: List[HubSpotWebhookEvent] = []
    for event in reversed(events):
        sub = event.subscriptionType or ""
        if (sub.endswith(".creation") or sub.endswith(".propertyChange")) and event.objectId is not None:
            key = (sub.split(".")[0], event.objectId)
            if key in seen:
                processed += 1
                continue
            seen.add(key)
        deduped.append(event)
    deduped.reverse()

    for event in deduped:
        try:
            object_id = str(event.objectId) if event.objectId is not None else ""
            sub_type = event.subscriptionType
            performed_by, performed_by_raw = _parse_actor(event)

            evt_kwargs = dict(
                performed_by=performed_by,
                performed_by_raw=performed_by_raw,
                hs_event_id=getattr(event, "eventId", None),
                hs_occurred_at=getattr(event, "occurredAt", None),
            )

            if sub_type.endswith(".propertyChange") and _is_ignored_property(event.propertyName):
                processed += 1
                continue

            # ── DEAL ──────────────────────────────────────────────────────────
            if sub_type == "deal.deletion":
                db = SessionLocal()
                try:
                    _soft_delete_deal(db, object_id, evt_kwargs)
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
                            _upsert_deals(db, [deal_data], stage_map,
                                          change_source="webhook", evt_kwargs=evt_kwargs)
                            db.commit()
                        finally:
                            db.close()
                finally:
                    await client.close()

            elif sub_type == "deal.associationChange":
                await _ensure_associated_record_exists(event, hubspot_token)
                db = SessionLocal()
                try:
                    _handle_association_change(db, event, evt_kwargs=evt_kwargs)
                    db.commit()
                finally:
                    db.close()

            # ── CONTACT property change ────────────────────────────────────────
            elif sub_type == "contact.propertyChange":
                db = SessionLocal()
                try:
                    exists = db.execute(
                        select(Contact).where(Contact.id == object_id)
                    ).scalar_one_or_none()
                finally:
                    db.close()

                if not exists:
                    # Only track contacts that are linked to our deals
                    processed += 1
                    continue

                client = HubSpotClient(access_token=hubspot_token)
                try:
                    contact_data = await _fetch_single_contact(client, object_id)
                    if contact_data:
                        db = SessionLocal()
                        try:
                            _upsert_contacts(
                                db, {object_id: contact_data.get("properties", {})},
                                change_source="webhook", evt_kwargs=evt_kwargs,
                            )
                            db.commit()
                        finally:
                            db.close()
                finally:
                    await client.close()

            # ── COMPANY property change ────────────────────────────────────────
            elif sub_type == "company.propertyChange":
                db = SessionLocal()
                try:
                    exists = db.execute(
                        select(Company).where(Company.id == object_id)
                    ).scalar_one_or_none()
                finally:
                    db.close()

                if not exists:
                    processed += 1
                    continue

                client = HubSpotClient(access_token=hubspot_token)
                try:
                    company_data = await _fetch_single_company(client, object_id)
                    if company_data:
                        db = SessionLocal()
                        try:
                            _upsert_companies(
                                db, {object_id: company_data.get("properties", {})},
                                change_source="webhook", evt_kwargs=evt_kwargs,
                            )
                            db.commit()
                        finally:
                            db.close()
                finally:
                    await client.close()

            # ── Intentionally ignored ─────────────────────────────────────────
            elif sub_type in (
                "contact.creation", "contact.associationChange",
                "contact.deletion",
                "company.creation", "company.associationChange",
                "company.deletion",
            ):
                logger.debug("Skipping %s (objectId=%s)", sub_type, object_id)

            else:
                logger.warning("Unhandled subscriptionType: %s", sub_type)

            processed += 1

        except Exception as exc:
            logger.error("Webhook error eventId=%s: %s", event.eventId, exc, exc_info=True)
            errors.append(f"eventId={event.eventId} type={event.subscriptionType}: {exc}")

    return {"processed": processed, "errors": errors}


# ═══════════════════════════════════════════════════════════════════════════════
# SOFT DELETE — DEAL  (deals keep deleted_at/is_active; contacts/companies do not)
# ═══════════════════════════════════════════════════════════════════════════════

def _soft_delete_deal(db: Session, deal_id: str, evt_kwargs: Optional[Dict] = None) -> None:
    """
    Soft-delete a deal and cascade-close all its active association rows.
    Contact and company records themselves are NOT touched.
    """
    evt_kwargs = evt_kwargs or {}
    now = now_ist()

    deal: Optional[Deal] = db.execute(select(Deal).where(Deal.id == deal_id)).scalar_one_or_none()
    owner = deal.hubspot_owner_id if deal else None

    db.execute(
        update(Deal).where(Deal.id == deal_id).values(
            deleted_at=now,
            is_active=False, updated_at=now,
        )
    )
    _write_audit(db, table_name="deals", record_id=deal_id,
                 action="deal deleted", deal_id=deal_id,
                 source="webhook_deletion", **evt_kwargs)

    # Cascade: close all active DealContact rows
    active_contacts = db.execute(
        select(DealContact).where(
            DealContact.deal_id == deal_id,
            DealContact.deleted_at.is_(None),
        )
    ).scalars().all()
    for dc in active_contacts:
        _write_audit(db, table_name="deal_contacts", record_id=str(dc.id),
                     action="contact removed", deal_id=deal_id,
                     related_table="contacts", related_id=dc.contact_id,
                     source="deal_deleted", **evt_kwargs)
    if active_contacts:
        db.execute(
            update(DealContact)
            .where(DealContact.deal_id == deal_id, DealContact.deleted_at.is_(None))
            .values(deleted_at=now, updated_at=now)
        )

    # Cascade: close all active DealCompany rows
    active_companies = db.execute(
        select(DealCompany).where(
            DealCompany.deal_id == deal_id,
            DealCompany.deleted_at.is_(None),
        )
    ).scalars().all()
    for dc in active_companies:
        _write_audit(db, table_name="deal_companies", record_id=str(dc.id),
                     action="company removed", deal_id=deal_id,
                     related_table="companies", related_id=dc.company_id,
                     source="deal_deleted", **evt_kwargs)
    if active_companies:
        db.execute(
            update(DealCompany)
            .where(DealCompany.deal_id == deal_id, DealCompany.deleted_at.is_(None))
            .values(deleted_at=now, updated_at=now)
        )

    db.commit()
    logger.info(
        "deal %s soft-deleted — %d contact / %d company association(s) closed",
        deal_id, len(active_contacts), len(active_companies),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PRE-FETCH ASSOCIATED RECORD (before saving association)
# ═══════════════════════════════════════════════════════════════════════════════

async def _ensure_associated_record_exists(
    event: HubSpotWebhookEvent,
    hubspot_token: str,
) -> None:
    """
    When a new association arrives, make sure the contact/company row exists in DB.
    If not, fetch it from HubSpot and insert it.
    """
    a_type = event.associationType or ""
    if event.associationRemoved or a_type not in ("DEAL_TO_CONTACT", "DEAL_TO_COMPANY"):
        return

    to_id = str(event.toObjectId) if event.toObjectId is not None else None
    if not to_id:
        return

    db = SessionLocal()
    try:
        if a_type == "DEAL_TO_CONTACT":
            exists = db.execute(select(Contact).where(Contact.id == to_id)).scalar_one_or_none()
            if not exists:
                client = HubSpotClient(access_token=hubspot_token)
                try:
                    data = await _fetch_single_contact(client, to_id)
                    if data:
                        _upsert_contacts(db, {to_id: data.get("properties", {})},
                                         change_source="webhook_assoc_prefetch")
                        db.commit()
                finally:
                    await client.close()
        else:
            exists = db.execute(select(Company).where(Company.id == to_id)).scalar_one_or_none()
            if not exists:
                client = HubSpotClient(access_token=hubspot_token)
                try:
                    data = await _fetch_single_company(client, to_id)
                    if data:
                        _upsert_companies(db, {to_id: data.get("properties", {})},
                                          change_source="webhook_assoc_prefetch")
                        db.commit()
                finally:
                    await client.close()
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# ASSOCIATION CHANGE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

def _handle_association_change(
    db: Session,
    event: HubSpotWebhookEvent,
    evt_kwargs: Optional[Dict] = None,
) -> None:
    """
    Handle deal.associationChange webhook.

    Writes "contact linked" / "contact removed" / "company linked" / "company removed"
    to the audit log. Does NOT write internal HubSpot codes like CRM_UI or USER.
    """
    evt_kwargs = evt_kwargs or {}
    a_type = event.associationType

    # Skip reverse-direction events
    if not a_type or a_type in ("CONTACT_TO_DEAL", "COMPANY_TO_DEAL"):
        return

    removed = bool(event.associationRemoved)
    is_primary = bool(event.isPrimaryAssociation)
    from_id = str(event.fromObjectId) if event.fromObjectId is not None else None
    to_id = str(event.toObjectId) if event.toObjectId is not None else None
    now = now_ist()

    if a_type == "DEAL_TO_CONTACT":
        deal_id, contact_id = from_id, to_id
        if not deal_id or not contact_id:
            return

        if removed:
            active_rows = db.execute(
                select(DealContact).where(
                    DealContact.deal_id == deal_id,
                    DealContact.contact_id == contact_id,
                    DealContact.deleted_at.is_(None),
                )
            ).scalars().all()
            for row in active_rows:
                _write_audit(
                    db, table_name="deal_contacts", record_id=str(row.id),
                    action="contact removed",
                    deal_id=deal_id, related_table="contacts", related_id=contact_id,
                    source="webhook_assoc", **evt_kwargs,
                )
            db.execute(
                update(DealContact)
                .where(DealContact.deal_id == deal_id,
                       DealContact.contact_id == contact_id,
                       DealContact.deleted_at.is_(None))
                .values(deleted_at=now, updated_at=now)
            )
        else:
            existing = db.query(DealContact).filter(
                DealContact.deal_id == deal_id,
                DealContact.contact_id == contact_id,
                DealContact.deleted_at.is_(None),
            ).first()
            if existing:
                existing.is_primary = is_primary
                existing.updated_at = now
            else:
                new_row = DealContact(
                    deal_id=deal_id, contact_id=contact_id,
                    association_type=a_type, is_primary=is_primary,
                    created_at=now, updated_at=now, deleted_at=None,
                )
                db.add(new_row)
                db.flush()
                _write_audit(
                    db, table_name="deal_contacts", record_id=str(new_row.id),
                    action="contact linked",
                    deal_id=deal_id, related_table="contacts", related_id=contact_id,
                    source="webhook_assoc", **evt_kwargs,
                )

    elif a_type == "DEAL_TO_COMPANY":
        deal_id, company_id = from_id, to_id
        if not deal_id or not company_id:
            return

        if removed:
            active_rows = db.execute(
                select(DealCompany).where(
                    DealCompany.deal_id == deal_id,
                    DealCompany.company_id == company_id,
                    DealCompany.deleted_at.is_(None),
                )
            ).scalars().all()
            for row in active_rows:
                _write_audit(
                    db, table_name="deal_companies", record_id=str(row.id),
                    action="company removed",
                    deal_id=deal_id, related_table="companies", related_id=company_id,
                    source="webhook_assoc", **evt_kwargs,
                )
            db.execute(
                update(DealCompany)
                .where(DealCompany.deal_id == deal_id,
                       DealCompany.company_id == company_id,
                       DealCompany.deleted_at.is_(None))
                .values(deleted_at=now, updated_at=now)
            )
        else:
            existing = db.query(DealCompany).filter(
                DealCompany.deal_id == deal_id,
                DealCompany.company_id == company_id,
                DealCompany.deleted_at.is_(None),
            ).first()
            if existing:
                existing.is_primary = is_primary
                existing.updated_at = now
            else:
                new_row = DealCompany(
                    deal_id=deal_id, company_id=company_id,
                    association_type=a_type, is_primary=is_primary,
                    created_at=now, updated_at=now, deleted_at=None,
                )
                db.add(new_row)
                db.flush()
                _write_audit(
                    db, table_name="deal_companies", record_id=str(new_row.id),
                    action="company linked",
                    deal_id=deal_id, related_table="companies", related_id=company_id,
                    source="webhook_assoc", **evt_kwargs,
                )


# ═══════════════════════════════════════════════════════════════════════════════
# UPSERT — DEALS
# ═══════════════════════════════════════════════════════════════════════════════

def _upsert_deals(
    db: Session,
    deals: Iterable[Dict],
    stage_map: Dict[str, str],
    change_source: str = "initial_load",
    evt_kwargs: Optional[Dict] = None,
) -> None:
    evt_kwargs = evt_kwargs or {}
    now = now_ist()

    for deal in deals:
        props = deal.get("properties", {})
        deal_id = str(deal.get("id", ""))
        stage_id = _get_prop(props, "dealstage") or ""
        owner = _get_prop(props, "hubspot_owner_id")

        new_values = {
            "dealname": _get_prop(props, "dealname"),
            "amount": _get_prop(props, "amount"),
            "dealstage": stage_id,
            "dealstage_label": stage_map.get(stage_id),
            "closedate": _get_prop(props, "closedate"),
            "pipeline": _get_prop(props, "pipeline"),
            "dealtype": _get_prop(props, "dealtype"),
            "description": _get_prop(props, "description"),
            "hubspot_owner_id": owner,
            "deal_owner_email": _get_prop(props, "deal_owner_email"),
            "delivery_owner": _get_prop(props, "delivery_owner"),
            "project_start_date": _get_prop(props, "project_start_date"),
            "project_end_date": _get_prop(props, "project_end_date"),
            "po_hours": _get_prop(props, "po_hours"),
        }

        existing: Optional[Deal] = db.execute(
            select(Deal).where(Deal.id == deal_id)
        ).scalar_one_or_none()

        if existing is None:
            new_deal = Deal(
                id=deal_id,
                hs_object_id=_get_prop(props, "hs_object_id") or deal_id,
                createdate=_get_prop(props, "createdate"),
                hs_lastmodifieddate=_get_prop(props, "hs_lastmodifieddate"),
                created_at=now,
                updated_at=now,
                is_active=True, deleted_at=None,
                **new_values,
            )
            db.add(new_deal)
            db.flush()
            _write_audit(
                db, table_name="deals", record_id=deal_id,
                action="deal created",
                changed_fields={k: {"old": None, "new": v} for k, v in new_values.items() if v is not None},
                deal_id=deal_id, source=change_source, **evt_kwargs,
            )
        else:
            diff = _diff_fields(existing, new_values, _DEAL_AUDIT_FIELDS)
            existing.hs_object_id = _get_prop(props, "hs_object_id") or deal_id
            existing.hs_lastmodifieddate = _get_prop(props, "hs_lastmodifieddate")
            existing.updated_at = now
            existing.deleted_at = None
            existing.is_active = True
            for f, v in new_values.items():
                setattr(existing, f, v)
            if diff:
                _write_audit(
                    db, table_name="deals", record_id=deal_id,
                    action="deal updated",
                    changed_fields=diff, deal_id=deal_id,
                    source=change_source, **evt_kwargs,
                )


# ═══════════════════════════════════════════════════════════════════════════════
# UPSERT — CONTACTS
# ═══════════════════════════════════════════════════════════════════════════════

def _upsert_contacts(
    db: Session,
    contacts_map: Dict[str, Dict],
    change_source: str = "initial_load",
    evt_kwargs: Optional[Dict] = None,
) -> None:
    """
    Upsert contacts.
    On update: write "contact updated" audit on the contacts table.
    Also write "contact updated" on each active deal_contacts row so the change
    is visible when querying audit by deal_id.
    """
    evt_kwargs = evt_kwargs or {}

    for contact_id, props in contacts_map.items():
        cid = str(contact_id)

        new_values = {
            "email": _get_prop(props, "email"),
            "firstname": _get_prop(props, "firstname"),
            "lastname": _get_prop(props, "lastname"),
            "hubspot_owner_id": _get_prop(props, "hubspot_owner_id"),
            "jobtitle": _get_prop(props, "jobtitle"),
            "phone": _get_prop(props, "phone"),
            "lifecyclestage": _get_prop(props, "lifecyclestage"),
            "lead_status": _get_prop(props, "hs_lead_status"),
        }

        existing: Optional[Contact] = db.execute(
            select(Contact).where(Contact.id == cid)
        ).scalar_one_or_none()

        if existing is None:
            db.add(Contact(
                id=cid,
                hs_object_id=_get_prop(props, "hs_object_id") or cid,
                createdate=_get_prop(props, "createdate"),
                lastmodifieddate=_get_prop(props, "lastmodifieddate"),
                **new_values,
            ))
            db.flush()
            _write_audit(
                db, table_name="contacts", record_id=cid,
                action="contact created",
                changed_fields={k: {"old": None, "new": v} for k, v in new_values.items() if v is not None},
                source=change_source, **evt_kwargs,
            )
        else:
            diff = _diff_fields(existing, new_values, _CONTACT_AUDIT_FIELDS)
            existing.hs_object_id = _get_prop(props, "hs_object_id") or cid
            existing.lastmodifieddate = _get_prop(props, "lastmodifieddate")
            for f, v in new_values.items():
                setattr(existing, f, v)

            if diff:
                # Primary audit entry on the contact itself
                _write_audit(
                    db, table_name="contacts", record_id=cid,
                    action="contact updated",
                    changed_fields=diff, source=change_source, **evt_kwargs,
                )
                # Mirror onto every active deal link so it surfaces in deal-scoped audit queries
                active_links = db.execute(
                    select(DealContact).where(
                        DealContact.contact_id == cid,
                        DealContact.deleted_at.is_(None),
                    )
                ).scalars().all()
                for dc in active_links:
                    _write_audit(
                        db, table_name="deal_contacts", record_id=str(dc.id),
                        action="contact updated",
                        changed_fields=diff,
                        deal_id=dc.deal_id, related_table="contacts", related_id=cid,
                        source=change_source, **evt_kwargs,
                    )


# ═══════════════════════════════════════════════════════════════════════════════
# UPSERT — COMPANIES
# ═══════════════════════════════════════════════════════════════════════════════

def _upsert_companies(
    db: Session,
    companies_map: Dict[str, Dict],
    change_source: str = "initial_load",
    evt_kwargs: Optional[Dict] = None,
) -> None:
    """
    Upsert companies.
    On update: write "company updated" audit on the companies table.
    Also write "company updated" on each active deal_companies row.
    """
    evt_kwargs = evt_kwargs or {}

    for company_id, props in companies_map.items():
        cid = str(company_id)

        new_values = {
            "domain": _get_prop(props, "domain"),
            "name": _get_prop(props, "name"),
            "hubspot_owner_id": _get_prop(props, "hubspot_owner_id"),
            "industry": _get_prop(props, "industry"),
            "company_type": _get_prop(props, "type"),
            "city": _get_prop(props, "city"),
            "state": _get_prop(props, "state"),
            "postal_code": _get_prop(props, "zip"),
            "number_of_employees": _get_prop(props, "numberofemployees"),
            "annual_revenue": _get_prop(props, "annualrevenue"),
            "timezone": _get_prop(props, "timezone"),
            "description": _get_prop(props, "description"),
            "linkedin_company_page": _get_prop(props, "linkedin_company_page"),
        }

        existing: Optional[Company] = db.execute(
            select(Company).where(Company.id == cid)
        ).scalar_one_or_none()

        if existing is None:
            db.add(Company(
                id=cid,
                hs_object_id=_get_prop(props, "hs_object_id") or cid,
                createdate=_get_prop(props, "createdate"),
                hs_lastmodifieddate=_get_prop(props, "hs_lastmodifieddate"),
                **new_values,
            ))
            db.flush()
            _write_audit(
                db, table_name="companies", record_id=cid,
                action="company created",
                changed_fields={k: {"old": None, "new": v} for k, v in new_values.items() if v is not None},
                source=change_source, **evt_kwargs,
            )
        else:
            diff = _diff_fields(existing, new_values, _COMPANY_AUDIT_FIELDS)
            existing.hs_object_id = _get_prop(props, "hs_object_id") or cid
            existing.hs_lastmodifieddate = _get_prop(props, "hs_lastmodifieddate")
            for f, v in new_values.items():
                setattr(existing, f, v)

            if diff:
                _write_audit(
                    db, table_name="companies", record_id=cid,
                    action="company updated",
                    changed_fields=diff, source=change_source, **evt_kwargs,
                )
                active_links = db.execute(
                    select(DealCompany).where(
                        DealCompany.company_id == cid,
                        DealCompany.deleted_at.is_(None),
                    )
                ).scalars().all()
                for dc in active_links:
                    _write_audit(
                        db, table_name="deal_companies", record_id=str(dc.id),
                        action="company updated",
                        changed_fields=diff,
                        deal_id=dc.deal_id, related_table="companies", related_id=cid,
                        source=change_source, **evt_kwargs,
                    )


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
    now = now_ist()
    for deal_id, contact_ids in contact_assoc.items():
        incoming = {str(c) for c in contact_ids}
        active_rows: List[DealContact] = db.query(DealContact).filter(
            DealContact.deal_id == str(deal_id),
            DealContact.deleted_at.is_(None),
        ).all()
        active_map = {row.contact_id: row for row in active_rows}

        for contact_id in set(active_map) - incoming:
            row = active_map[contact_id]
            row.deleted_at = now
            row.updated_at = now
            _write_audit(db, table_name="deal_contacts", record_id=str(row.id),
                         action="contact removed", deal_id=str(deal_id),
                         related_table="contacts", related_id=contact_id,
                         source="initial_load_replace")

        for contact_id in incoming:
            if contact_id in active_map:
                active_map[contact_id].updated_at = now
            else:
                new_row = DealContact(
                    deal_id=str(deal_id), contact_id=contact_id,
                    association_type="DEAL_TO_CONTACT",
                    created_at=now, updated_at=now, deleted_at=None,
                )
                db.add(new_row)
                db.flush()
                _write_audit(db, table_name="deal_contacts", record_id=str(new_row.id),
                             action="contact linked", deal_id=str(deal_id),
                             related_table="contacts", related_id=contact_id,
                             source="initial_load")


def _replace_deal_companies(db: Session, company_assoc: Dict[str, List[str]]) -> None:
    now = now_ist()
    for deal_id, company_ids in company_assoc.items():
        incoming = {str(c) for c in company_ids}
        active_rows: List[DealCompany] = db.query(DealCompany).filter(
            DealCompany.deal_id == str(deal_id),
            DealCompany.deleted_at.is_(None),
        ).all()
        active_map = {row.company_id: row for row in active_rows}

        for company_id in set(active_map) - incoming:
            row = active_map[company_id]
            row.deleted_at = now
            row.updated_at = now
            _write_audit(db, table_name="deal_companies", record_id=str(row.id),
                         action="company removed", deal_id=str(deal_id),
                         related_table="companies", related_id=company_id,
                         source="initial_load_replace")

        for company_id in incoming:
            if company_id in active_map:
                active_map[company_id].updated_at = now
            else:
                new_row = DealCompany(
                    deal_id=str(deal_id), company_id=company_id,
                    association_type="DEAL_TO_COMPANY",
                    created_at=now, updated_at=now, deleted_at=None,
                )
                db.add(new_row)
                db.flush()
                _write_audit(db, table_name="deal_companies", record_id=str(new_row.id),
                             action="company linked", deal_id=str(deal_id),
                             related_table="companies", related_id=company_id,
                             source="initial_load")


def _replace_attachments(db: Session, attachments_map: Dict[str, List]) -> None:
    deal_ids = list(attachments_map.keys())
    for chunk in _chunk_list(deal_ids, 500):
        db.execute(delete(Attachment).where(Attachment.deal_id.in_(chunk)))
    now = now_ist()
    rows = []
    for deal_id, attachments in attachments_map.items():
        for att in attachments:
            rows.append(Attachment(
                deal_id=str(att.get("deal_id", deal_id)),
                note_id=str(att.get("note_id", "")),
                file_id=str(att.get("file_id", "")),
                created_at=now, created_by="initial_load",
                updated_at=now, updated_by="initial_load",
                is_active=True,
            ))
    db.add_all(rows)


# ═══════════════════════════════════════════════════════════════════════════════
# HUBSPOT API FETCH HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

async def _fetch_single_deal(client: HubSpotClient, deal_id: str) -> Optional[Dict]:
    try:
        data = await client.batch_read_objects("deals", [deal_id], DEAL_PROPERTIES)
        results = data.get("results", [])
        return results[0] if results else None
    except Exception as exc:
        logger.error("fetch deal %s failed: %s", deal_id, exc)
        return None


async def _fetch_single_contact(client: HubSpotClient, contact_id: str) -> Optional[Dict]:
    try:
        data = await client.batch_read_objects("contacts", [contact_id], CONTACT_PROPERTIES)
        results = data.get("results", [])
        return results[0] if results else None
    except Exception as exc:
        logger.error("fetch contact %s failed: %s", contact_id, exc)
        return None


async def _fetch_single_company(client: HubSpotClient, company_id: str) -> Optional[Dict]:
    try:
        data = await client.batch_read_objects("companies", [company_id], COMPANY_PROPERTIES)
        results = data.get("results", [])
        return results[0] if results else None
    except Exception as exc:
        logger.error("fetch company %s failed: %s", company_id, exc)
        return None


async def _get_stage_map(client: HubSpotClient) -> Dict[str, str]:
    try:
        data = await client.get_pipeline_stages("79964941")
        return {s["id"]: s["label"] for s in data.get("results", [])}
    except Exception as exc:
        logger.error("fetch stage map failed: %s", exc)
        return {}


# ═══════════════════════════════════════════════════════════════════════════════
# JOB HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def create_job(db: Session, job_id: str, started_at) -> Job:
    job = Job(job_id=job_id, status=LoadStatus.RUNNING.value, started_at=started_at)
    db.add(job)
    db.commit()
    return job


def update_job_from_progress(db: Session, job_id: str, progress: LoadProgress) -> Job:
    job = db.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none()
    if not job:
        job = Job(job_id=job_id, started_at=now_ist())
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
        from datetime import datetime
        job.completed_at = datetime.fromisoformat(progress.completed_at)
    db.commit()
    return job


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _chunk_list(items: List, size: int) -> List[List]:
    return [items[i: i + size] for i in range(0, len(items), size)]


def _get_prop(props: Dict, key: str) -> Optional[str]:
    value = props.get(key)
    if value is None:
        return None
    s = str(value).strip()
    return s or None