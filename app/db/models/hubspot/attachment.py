"""Attachment ORM model — production-ready."""

from datetime import datetime

from sqlalchemy import Column, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.sql import func

from app.db.base import Base


class Attachment(Base):
    __tablename__ = "attachments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_id = Column(String(64), nullable=False)
    note_id = Column(String(64), nullable=False)
    file_id = Column(String(64), nullable=False)

    synced_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        server_default=func.now(),
    )
    deleted_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("deal_id", "note_id", "file_id", name="uq_attachment"),
        Index("ix_attachments_deal_id", "deal_id"),
        Index("ix_attachments_deleted_at", "deleted_at"),
    )

    def __repr__(self) -> str:
        return f"<Attachment deal={self.deal_id} file={self.file_id}>"