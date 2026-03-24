"""Deal ORM model — production-ready."""

from datetime import datetime

from sqlalchemy import Column, DateTime, Index, String, Text
from sqlalchemy.sql import func

from app.db.base import Base


class Deal(Base):
    __tablename__ = "deals"

    # ── Primary key ───────────────────────────────────────────────────────────
    id = Column(String(64), primary_key=True, comment="HubSpot deal ID (hs_object_id)")

    # ── HubSpot identity ──────────────────────────────────────────────────────
    hs_object_id = Column(String(64), nullable=True, index=True)
    portal_id = Column(String(32), nullable=True, comment="HubSpot portal / account ID")

    # ── Core deal fields ──────────────────────────────────────────────────────
    dealname = Column(String(512), nullable=True)
    amount = Column(String(64), nullable=True, comment="Raw string from HubSpot; cast at query time")
    dealstage = Column(String(128), nullable=True, comment="Stage ID from HubSpot pipeline")
    dealstage_label = Column(String(256), nullable=True, comment="Human-readable stage name")
    pipeline = Column(String(128), nullable=True)
    dealtype = Column(String(128), nullable=True)
    description = Column(Text, nullable=True)
    closedate = Column(String(32), nullable=True)
    createdate = Column(String(32), nullable=True)
    hs_lastmodifieddate = Column(String(32), nullable=True)

    # ── Ownership ─────────────────────────────────────────────────────────────
    hubspot_owner_id = Column(String(64), nullable=True)
    deal_owner_email = Column(String(256), nullable=True)
    delivery_owner = Column(String(256), nullable=True)

    # ── Project scheduling ────────────────────────────────────────────────────
    project_start_date = Column(String(32), nullable=True)
    project_end_date = Column(String(32), nullable=True)
    po_hours = Column(String(32), nullable=True)

    # ── Audit / soft-delete ───────────────────────────────────────────────────
    # synced_at  : last time our code wrote this row (set by DB trigger / app)
    # deleted_at : NULL = active, non-NULL = soft-deleted
    synced_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        server_default=func.now(),
        comment="Last time this row was written by our sync",
    )
    deleted_at = Column(DateTime, nullable=True, comment="Soft-delete timestamp; NULL = active")

    # ── Composite indexes for common queries ──────────────────────────────────
    __table_args__ = (
        Index("ix_deals_pipeline_stage", "pipeline", "dealstage"),
        Index("ix_deals_deleted_at", "deleted_at"),
        Index("ix_deals_owner", "hubspot_owner_id"),
    )

    def __repr__(self) -> str:
        return f"<Deal id={self.id} name={self.dealname!r} stage={self.dealstage_label!r}>"