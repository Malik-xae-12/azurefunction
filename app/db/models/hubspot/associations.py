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

from sqlalchemy import Boolean, Column, DateTime, Index, String,Integer
from sqlalchemy.sql import func

from app.db.base import Base
from sqlalchemy.ext.hybrid import hybrid_property

class DealContact(Base):
    """
    Many-to-many between deals and contacts with full audit history.
 
    FIX 2: Uses a surrogate integer PK (`id`) instead of a composite PK on
    (deal_id, contact_id).  This allows multiple historical rows for the same
    pair — e.g. the original link, a deletion, and a fresh re-link each get
    their own row with independent timestamps.
 
    A partial unique index (deal_id, contact_id) WHERE deleted_at IS NULL
    enforces that at most one *active* row exists per pair at the DB level,
    which prevents phantom duplicates without sacrificing history.
    """
    __tablename__ = "deal_contacts"
 
    # ── Surrogate PK (FIX 2) ─────────────────────────────────────────────────
    id = Column(Integer, primary_key=True, autoincrement=True)
 
    # ── Business key ─────────────────────────────────────────────────────────
    deal_id = Column(String(64), nullable=False, comment="HubSpot deal ID")
    contact_id = Column(String(64), nullable=False, comment="HubSpot contact ID")
 
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
 
    # ── FIX 3: portal_id ──────────────────────────────────────────────────────
    portal_id = Column(
        String(32),
        nullable=True,
        comment="HubSpot portalId from the webhook event that created this row",
    )
 
    # ── Source tracking ───────────────────────────────────────────────────────
    change_source = Column(
        String(64),
        nullable=True,
        default="initial_load",
        comment="'initial_load' | 'webhook' | 'webhook_assoc_prefetch' etc.",
    )
 
    # ── Audit timestamps ──────────────────────────────────────────────────────
    created_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
        comment="When this association row was inserted",
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
        comment="Soft-delete timestamp; NULL = this link is currently active",
    )
 
    __table_args__ = (
        # Fast lookups by either side of the relationship
        Index("ix_deal_contacts_deal_id", "deal_id"),
        Index("ix_deal_contacts_contact_id", "contact_id"),
        Index("ix_deal_contacts_deleted_at", "deleted_at"),
        # FIX 2: enforce one active row per pair at DB level (PostgreSQL syntax).
        # For MySQL, remove postgresql_where and enforce uniqueness in service code.
        Index(
            "uq_deal_contacts_active_pair",
            "deal_id",
            "contact_id",
            unique=True,
            postgresql_where=Column("deleted_at").is_(None),
        ),
    )
 
    # ── FIX 4: is_active hybrid property ─────────────────────────────────────
    @hybrid_property
    def is_active(self) -> bool:
        """True when this association has not been soft-deleted."""
        return self.deleted_at is None
 
    @is_active.expression
    def is_active(cls):  # noqa: N805
        return cls.deleted_at.is_(None)
 
    def __repr__(self) -> str:
        status = "active" if self.deleted_at is None else "deleted"
        return f"<DealContact id={self.id} deal={self.deal_id} contact={self.contact_id} [{status}]>"
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# DEAL ↔ COMPANY  (FIX 2: surrogate PK replaces composite PK)
# ═══════════════════════════════════════════════════════════════════════════════
 
class DealCompany(Base):
    """
    Many-to-many between deals and companies with full audit history.
 
    Same design as DealContact — see that class for rationale.
    """
    __tablename__ = "deal_companies"
 
    # ── Surrogate PK (FIX 2) ─────────────────────────────────────────────────
    id = Column(Integer, primary_key=True, autoincrement=True)
 
    # ── Business key ─────────────────────────────────────────────────────────
    deal_id = Column(String(64), nullable=False, comment="HubSpot deal ID")
    company_id = Column(String(64), nullable=False, comment="HubSpot company ID")
 
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
 
    # ── FIX 3: portal_id ──────────────────────────────────────────────────────
    portal_id = Column(
        String(32),
        nullable=True,
        comment="HubSpot portalId from the webhook event that created this row",
    )
 
    # ── Source tracking ───────────────────────────────────────────────────────
    change_source = Column(
        String(64),
        nullable=True,
        default="initial_load",
    )
 
    # ── Audit timestamps ──────────────────────────────────────────────────────
    created_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        server_default=func.now(),
    )
    deleted_at = Column(
        DateTime,
        nullable=True,
        comment="Soft-delete timestamp; NULL = this link is currently active",
    )
 
    __table_args__ = (
        Index("ix_deal_companies_deal_id", "deal_id"),
        Index("ix_deal_companies_company_id", "company_id"),
        Index("ix_deal_companies_deleted_at", "deleted_at"),
        # FIX 2: enforce one active row per pair (PostgreSQL partial unique index).
        Index(
            "uq_deal_companies_active_pair",
            "deal_id",
            "company_id",
            unique=True,
            postgresql_where=Column("deleted_at").is_(None),
        ),
    )
 
    # ── FIX 4: is_active hybrid property ─────────────────────────────────────
    @hybrid_property
    def is_active(self) -> bool:
        """True when this association has not been soft-deleted."""
        return self.deleted_at is None
 
    @is_active.expression
    def is_active(cls):  # noqa: N805
        return cls.deleted_at.is_(None)
 
    def __repr__(self) -> str:
        status = "active" if self.deleted_at is None else "deleted"
        return f"<DealCompany id={self.id} deal={self.deal_id} company={self.company_id} [{status}]>"
 