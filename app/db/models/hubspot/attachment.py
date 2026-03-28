"""Attachment ORM model — production-ready with full audit trail. All timestamps in IST."""

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

    # ── Audit columns (all IST) ───────────────────────────────────────────────
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=now_ist,
        server_default=func.now(),
        comment="When this row was first inserted (IST)",
    )
    created_by = Column(String(64), nullable=True, comment="System source that created this row")
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=now_ist,
        onupdate=now_ist,
        server_default=func.now(),
        comment="Last time this row was modified (IST)",
    )
    updated_by = Column(String(64), nullable=True)
    deleted_at = Column(DateTime(timezone=True), nullable=True, comment="Soft-delete timestamp (IST)")
    deleted_by = Column(String(64), nullable=True)
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

    def soft_delete(self, by: str = None) -> None:
        now = now_ist()
        self.deleted_at = now
        self.deleted_by = by
        self.is_active = False
        self.updated_at = now
        self.updated_by = by

    def __repr__(self) -> str:
        return f"<Attachment deal={self.deal_id} file={self.file_id} active={self.is_active}>"