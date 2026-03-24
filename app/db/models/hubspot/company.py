"""Company ORM model — production-ready."""

from datetime import datetime

from sqlalchemy import Column, DateTime, Index, String
from sqlalchemy.sql import func

from app.db.base import Base


class Company(Base):
    __tablename__ = "companies"

    # ── Primary key ───────────────────────────────────────────────────────────
    id = Column(String(64), primary_key=True, comment="HubSpot company ID")

    # ── HubSpot identity ──────────────────────────────────────────────────────
    hs_object_id = Column(String(64), nullable=True, index=True)

    # ── Core company fields ───────────────────────────────────────────────────
    name = Column(String(512), nullable=True)
    domain = Column(String(256), nullable=True, index=True)
    industry = Column(String(256), nullable=True)
    city = Column(String(128), nullable=True)
    country = Column(String(128), nullable=True)

    # ── Timestamps ────────────────────────────────────────────────────────────
    createdate = Column(String(32), nullable=True)
    hs_lastmodifieddate = Column(String(32), nullable=True)

    # ── Audit / soft-delete ───────────────────────────────────────────────────
    synced_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        server_default=func.now(),
    )
    deleted_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_companies_deleted_at", "deleted_at"),
    )

    def __repr__(self) -> str:
        return f"<Company id={self.id} name={self.name!r}>"