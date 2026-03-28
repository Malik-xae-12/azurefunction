"""
Deal association tables — DealContact and DealCompany.

Timestamp-only audit. No is_active, no change_source, no created_by/updated_by/deleted_by.
Who performed each action is stored exclusively in hubspot_audit_log.

Timestamp semantics:
  created_at  — when the association was first linked
  updated_at  — when is_primary changed or row was re-upserted
  deleted_at  — when the association was removed (NULL = still active)
"""

from sqlalchemy import Boolean, Column, DateTime, Index, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property

from app.db.base import Base
from app.utils.timezone import now_ist


class DealContact(Base):
    """
    Many-to-many between deals and contacts.

    One active row per (deal_id, contact_id) enforced via partial unique index on deleted_at IS NULL.
    deleted_at is set when:
      1. Association explicitly removed (deal.associationChange, associationRemoved=true)
      2. Deal deleted from HubSpot (deal.deletion webhook cascade)
    Note: contacts table has no soft-delete; contact records are simply deleted/updated in place.
    """
    __tablename__ = "deal_contacts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_id = Column(String(64), nullable=False, comment="HubSpot deal ID")
    contact_id = Column(String(64), nullable=False, comment="HubSpot contact ID")
    association_type = Column(String(128), nullable=True, comment="DEAL_TO_CONTACT")
    is_primary = Column(Boolean, nullable=True, default=False)

    created_at = Column(
        DateTime(timezone=True), nullable=False, default=now_ist,
        comment="When this association was created (IST)",
    )
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=now_ist, onupdate=now_ist,
        comment="When this row was last touched (IST)",
    )
    deleted_at = Column(
        DateTime(timezone=True), nullable=True,
        comment="When this association was removed (IST). NULL = active.",
    )

    __table_args__ = (
        Index("ix_deal_contacts_deal_id", "deal_id"),
        Index("ix_deal_contacts_contact_id", "contact_id"),
        Index("ix_deal_contacts_deleted_at", "deleted_at"),
        Index(
            "uq_deal_contacts_active_pair",
            "deal_id", "contact_id",
            unique=True,
            postgresql_where=Column("deleted_at").is_(None),
        ),
    )

    @hybrid_property
    def is_active(self) -> bool:
        return self.deleted_at is None

    @is_active.expression
    def is_active(cls):  # noqa: N805
        return cls.deleted_at.is_(None)

    def soft_delete(self) -> None:
        now = now_ist()
        self.deleted_at = now
        self.updated_at = now

    def __repr__(self) -> str:
        status = "active" if self.deleted_at is None else "removed"
        return f"<DealContact id={self.id} deal={self.deal_id} contact={self.contact_id} [{status}]>"


class DealCompany(Base):
    """
    Many-to-many between deals and companies.

    deleted_at is set when:
      1. Association explicitly removed (deal.associationChange, associationRemoved=true)
      2. Deal deleted from HubSpot (deal.deletion webhook cascade)
    """
    __tablename__ = "deal_companies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_id = Column(String(64), nullable=False, comment="HubSpot deal ID")
    company_id = Column(String(64), nullable=False, comment="HubSpot company ID")
    association_type = Column(String(128), nullable=True, comment="DEAL_TO_COMPANY")
    is_primary = Column(Boolean, nullable=True, default=False)

    created_at = Column(
        DateTime(timezone=True), nullable=False, default=now_ist,
        comment="When this association was created (IST)",
    )
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=now_ist, onupdate=now_ist,
        comment="When this row was last touched (IST)",
    )
    deleted_at = Column(
        DateTime(timezone=True), nullable=True,
        comment="When this association was removed (IST). NULL = active.",
    )

    __table_args__ = (
        Index("ix_deal_companies_deal_id", "deal_id"),
        Index("ix_deal_companies_company_id", "company_id"),
        Index("ix_deal_companies_deleted_at", "deleted_at"),
        Index(
            "uq_deal_companies_active_pair",
            "deal_id", "company_id",
            unique=True,
            postgresql_where=Column("deleted_at").is_(None),
        ),
    )

    @hybrid_property
    def is_active(self) -> bool:
        return self.deleted_at is None

    @is_active.expression
    def is_active(cls):  # noqa: N805
        return cls.deleted_at.is_(None)

    def soft_delete(self) -> None:
        now = now_ist()
        self.deleted_at = now
        self.updated_at = now

    def __repr__(self) -> str:
        status = "active" if self.deleted_at is None else "removed"
        return f"<DealCompany id={self.id} deal={self.deal_id} company={self.company_id} [{status}]>"