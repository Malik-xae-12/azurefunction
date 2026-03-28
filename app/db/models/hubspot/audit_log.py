"""
HubSpot Audit Log — append-only, one row per meaningful state change.

action values (human-readable, no internal codes):
  deal created            — new deal inserted
  deal updated            — deal fields changed (diff in changed_fields)
  deal deleted            — deal soft-deleted
  contact created         — new contact inserted
  contact updated         — contact fields changed
  contact removed         — contact association removed from a deal
  company created         — new company inserted
  company updated         — company fields changed
  company removed         — company association removed from a deal
  contact linked          — contact associated to a deal
  company linked          — company associated to a deal

performed_by      : HubSpot userId extracted from webhook sourceId ("userId:89164629" → "89164629")
performed_by_raw  : full raw sourceId string for traceability
hs_event_id       : HubSpot eventId from webhook payload
hs_occurred_at    : occurredAt epoch ms from HubSpot webhook
"""

from sqlalchemy import Column, DateTime, Index, Integer, String, Text

from app.db.base import Base
from app.utils.timezone import now_ist


class AuditLog(Base):
    __tablename__ = "hubspot_audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ── What was affected ─────────────────────────────────────────────────────
    table_name = Column(
        String(64), nullable=False,
        comment="deals | contacts | companies | deal_contacts | deal_companies | attachments",
    )
    record_id = Column(
        String(64), nullable=False,
        comment="PK of the affected row in table_name",
    )

    # ── What happened (human-readable) ────────────────────────────────────────
    action = Column(
        String(64), nullable=False,
        comment=(
            "deal created | deal updated | deal deleted | "
            "contact created | contact updated | contact removed | contact linked | "
            "company created | company updated | company removed | company linked"
        ),
    )

    # ── Field-level diff (for *updated actions) ───────────────────────────────
    changed_fields = Column(
        Text, nullable=True,
        comment='JSON: {"dealname": {"old": "Foo", "new": "Bar"}}',
    )

    # ── Association context ───────────────────────────────────────────────────
    deal_id = Column(String(64), nullable=True, comment="deal involved in this event")
    related_table = Column(String(32), nullable=True, comment="'contacts' or 'companies'")
    related_id = Column(String(64), nullable=True, comment="contact_id or company_id")

    # ── Who performed the action ──────────────────────────────────────────────
    performed_by = Column(
        String(64), nullable=True,
        comment=(
            "HubSpot userId from webhook sourceId (e.g. '89164629'). "
            "NULL for system events (automation, cascade, initial_load)."
        ),
    )
    performed_by_raw = Column(
        String(256), nullable=True,
        comment="Full raw sourceId string from webhook, e.g. 'userId:89164629'",
    )

    # ── Source of the event ───────────────────────────────────────────────────
    source = Column(
        String(64), nullable=True,
        comment="initial_load | webhook | webhook_deletion | webhook_assoc | deal_deleted",
    )

    # ── HubSpot webhook metadata ──────────────────────────────────────────────
    hs_event_id = Column(
        String(64), nullable=True,
        comment="HubSpot eventId from webhook payload",
    )
    hs_occurred_at = Column(
        String(32), nullable=True,
        comment="occurredAt epoch ms from HubSpot — when HubSpot recorded the change",
    )

    # ── When this log row was written (IST) ───────────────────────────────────
    created_at = Column(
        DateTime(timezone=True), nullable=False, default=now_ist,
        comment="When this log entry was written (IST, UTC+5:30)",
    )

    __table_args__ = (
        Index("ix_audit_record", "table_name", "record_id"),
        Index("ix_audit_action", "action"),
        Index("ix_audit_deal_id", "deal_id"),
        Index("ix_audit_created_at", "created_at"),
        Index("ix_audit_performed_by", "performed_by"),
        Index("ix_audit_source", "source"),
    )

    def __repr__(self) -> str:
        return (
            f"<AuditLog id={self.id} action={self.action!r} "
            f"record={self.table_name}/{self.record_id} by={self.performed_by}>"
        )