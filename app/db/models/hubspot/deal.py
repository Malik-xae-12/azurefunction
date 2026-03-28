"""Deal ORM model — production-ready with full audit trail. All timestamps in IST."""

from sqlalchemy import Boolean, Column, DateTime, Index, String, Text
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import func

from app.db.base import Base
from app.utils.timezone import now_ist


class Deal(Base):
    __tablename__ = "deals"

    # ── Primary key ───────────────────────────────────────────────────────────
    id = Column(String(64), primary_key=True, comment="HubSpot deal ID (hs_object_id)")

    # ── HubSpot identity ──────────────────────────────────────────────────────
    hs_object_id = Column(String(64), nullable=True, index=True)

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

    # ── Audit columns (all IST) ───────────────────────────────────────────────
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=now_ist,
        server_default=func.now(),
        comment="When this row was first inserted (IST)",
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=now_ist,
        onupdate=now_ist,
        server_default=func.now(),
        comment="Last time this row was modified (IST)",
    )
    deleted_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Soft-delete timestamp (IST); NULL = deal is active",
    )
    is_active = Column(
        Boolean,
        nullable=False,
        default=True,
        server_default="1",
        comment="1 = active, 0 = soft-deleted. Always kept in sync with deleted_at.",
    )

    __table_args__ = (
        Index("ix_deals_pipeline_stage", "pipeline", "dealstage"),
        Index("ix_deals_deleted_at", "deleted_at"),
        Index("ix_deals_owner", "hubspot_owner_id"),
        Index("ix_deals_is_active", "is_active"),
    )

    @hybrid_property
    def is_deleted(self) -> bool:
        return self.deleted_at is not None

    @is_deleted.expression
    def is_deleted(cls):  # noqa: N805
        return cls.deleted_at.isnot(None)

    def soft_delete(self, by: str = None) -> None:
        """Soft-delete this deal. Flush/commit after calling."""
        now = now_ist()
        self.deleted_at = now
        self.is_active = False
        self.updated_at = now

    def __repr__(self) -> str:
        return (
            f"<Deal id={self.id} name={self.dealname!r} "
            f"stage={self.dealstage_label!r} active={self.is_active}>"
        )