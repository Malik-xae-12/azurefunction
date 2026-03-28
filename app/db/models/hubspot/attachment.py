"""Attachment ORM model — tracks files attached to deals via HubSpot notes."""

from sqlalchemy import Boolean, Column, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.sql import func

from app.db.base import Base
from app.utils.timezone import now_ist


class Attachment(Base):
    __tablename__ = "attachments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_id = Column(String(64), nullable=False)
    note_id = Column(String(64), nullable=False)
    file_id = Column(String(64), nullable=False)

    # ── File metadata from HubSpot ───────────────────────────────────────────
    file_name = Column(String(512), nullable=True, comment="Original file name from HubSpot")
    file_url = Column(String(1024), nullable=True, comment="HubSpot file URL")
    file_type = Column(String(128), nullable=True, comment="MIME type or extension e.g. application/pdf")

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
    deleted_at = Column(DateTime(timezone=True), nullable=True, comment="Soft-delete timestamp (IST)")
    is_active = Column(
        Boolean,
        nullable=False,
        default=True,
        server_default="1",
        comment="1 = active, 0 = soft-deleted",
    )

    __table_args__ = (
        UniqueConstraint("deal_id", "note_id", "file_id", name="uq_attachment"),
        Index("ix_attachments_deal_id", "deal_id"),
        Index("ix_attachments_deleted_at", "deleted_at"),
        Index("ix_attachments_is_active", "is_active"),
    )

    def soft_delete(self) -> None:
        now = now_ist()
        self.deleted_at = now
        self.is_active = False
        self.updated_at = now

    def __repr__(self) -> str:
        return f"<Attachment deal={self.deal_id} file={self.file_id} name={self.file_name!r} active={self.is_active}>"