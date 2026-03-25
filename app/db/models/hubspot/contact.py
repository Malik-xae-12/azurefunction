"""Contact ORM model — production-ready."""

from datetime import datetime

from sqlalchemy import Column, DateTime, Index, String
from sqlalchemy.sql import func
from sqlalchemy.ext.hybrid import hybrid_property
from app.db.base import Base

class Contact(Base):
    __tablename__ = "contacts"
 
    id = Column(String(64), primary_key=True, comment="HubSpot contact ID")
    hs_object_id = Column(String(64), nullable=True, index=True)
 
    email = Column(String(320), nullable=True, index=True)
    firstname = Column(String(256), nullable=True)
    lastname = Column(String(256), nullable=True)
    phone = Column(String(64), nullable=True)
    mobilephone = Column(String(64), nullable=True)
    city = Column(String(128), nullable=True)
    country = Column(String(128), nullable=True)
    company = Column(String(512), nullable=True, comment="Company name field on the contact")
    closedate = Column(String(32), nullable=True)
    createdate = Column(String(32), nullable=True)
    lastmodifieddate = Column(String(32), nullable=True)
 
    synced_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        server_default=func.now(),
    )
    deleted_at = Column(DateTime, nullable=True)
 
    __table_args__ = (
        Index("ix_contacts_deleted_at", "deleted_at"),
    )
 
    # ── FIX 4 ─────────────────────────────────────────────────────────────────
    @hybrid_property
    def is_active(self) -> bool:
        return self.deleted_at is None
 
    @is_active.expression
    def is_active(cls):  # noqa: N805
        return cls.deleted_at.is_(None)
 
    def __repr__(self) -> str:
        return f"<Contact id={self.id} email={self.email!r}>"