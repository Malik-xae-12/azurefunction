"""
Deal association tables — production-ready with full audit trail.

DealContact  : many-to-many between deals and contacts
DealCompany  : many-to-many between deals and companies

Design decisions:
  - Composite PK (deal_id + contact/company_id) prevents duplicate rows.
  - deleted_at supports soft-deletes triggered by associationChange webhooks.
  - association_type stores the HubSpot association label (e.g. "DEAL_TO_CONTACT").
  - is_primary tracks isPrimaryAssociation from HubSpot.
  - created_at / updated_at give a full timeline of every association change.
  - change_source records whether the row was written by initial load or a webhook.
"""

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Index, String
from sqlalchemy.sql import func

from app.db.base import Base


class DealContact(Base):
    __tablename__ = "deal_contacts"

    # ── Composite primary key ─────────────────────────────────────────────────
    deal_id = Column(String(64), primary_key=True, comment="HubSpot deal ID")
    contact_id = Column(String(64), primary_key=True, comment="HubSpot contact ID")

    # ── Association metadata ──────────────────────────────────────────────────
    association_type = Column(
        String(128),
        nullable=True,
        comment="HubSpot associationType, e.g. DEAL_TO_CONTACT",
    )
    is_primary = Column(
        Boolean,
        nullable=True,
        default=False,
        comment="isPrimaryAssociation from HubSpot webhook",
    )

    # ── Source tracking ───────────────────────────────────────────────────────
    change_source = Column(
        String(64),
        nullable=True,
        default="initial_load",
        comment="'initial_load' | 'webhook' — what created/updated this row",
    )

    # ── Audit timestamps ──────────────────────────────────────────────────────
    created_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
        comment="When this association was first recorded",
    )
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        server_default=func.now(),
        comment="Last time this row was touched",
    )
    deleted_at = Column(
        DateTime,
        nullable=True,
        comment="Soft-delete timestamp set when HubSpot fires associationChange with associationRemoved=True",
    )

    __table_args__ = (
        Index("ix_deal_contacts_deal_id", "deal_id"),
        Index("ix_deal_contacts_contact_id", "contact_id"),
        Index("ix_deal_contacts_deleted_at", "deleted_at"),
    )

    def __repr__(self) -> str:
        status = "deleted" if self.deleted_at else "active"
        return f"<DealContact deal={self.deal_id} contact={self.contact_id} [{status}]>"


class DealCompany(Base):
    __tablename__ = "deal_companies"

    # ── Composite primary key ─────────────────────────────────────────────────
    deal_id = Column(String(64), primary_key=True, comment="HubSpot deal ID")
    company_id = Column(String(64), primary_key=True, comment="HubSpot company ID")

    # ── Association metadata ──────────────────────────────────────────────────
    association_type = Column(
        String(128),
        nullable=True,
        comment="HubSpot associationType, e.g. DEAL_TO_COMPANY",
    )
    is_primary = Column(
        Boolean,
        nullable=True,
        default=False,
        comment="isPrimaryAssociation from HubSpot webhook",
    )

    # ── Source tracking ───────────────────────────────────────────────────────
    change_source = Column(
        String(64),
        nullable=True,
        default="initial_load",
        comment="'initial_load' | 'webhook' — what created/updated this row",
    )

    # ── Audit timestamps ──────────────────────────────────────────────────────
    created_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
        comment="When this association was first recorded",
    )
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        server_default=func.now(),
        comment="Last time this row was touched",
    )
    deleted_at = Column(
        DateTime,
        nullable=True,
        comment="Soft-delete timestamp set when HubSpot fires associationChange with associationRemoved=True",
    )

    __table_args__ = (
        Index("ix_deal_companies_deal_id", "deal_id"),
        Index("ix_deal_companies_company_id", "company_id"),
        Index("ix_deal_companies_deleted_at", "deleted_at"),
    )

    def __repr__(self) -> str:
        status = "deleted" if self.deleted_at else "active"
        return f"<DealCompany deal={self.deal_id} company={self.company_id} [{status}]>"