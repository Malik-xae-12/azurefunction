"""Contact ORM model — columns match HubSpot Create Contact form exactly. No soft-delete columns."""

from sqlalchemy import Column, Index, String

from app.db.base import Base


class Contact(Base):
    __tablename__ = "contacts"

    # ── Primary key ───────────────────────────────────────────────────────────
    id = Column(String(64), primary_key=True, comment="HubSpot contact ID (hs_object_id)")
    hs_object_id = Column(String(64), nullable=True, index=True)

    # ── Fields matching HubSpot Create Contact form (top to bottom) ───────────
    email = Column(String(320), nullable=True, index=True)
    firstname = Column(String(256), nullable=True)
    lastname = Column(String(256), nullable=True)
    hubspot_owner_id = Column(String(64), nullable=True, comment="Contact owner — HubSpot user ID")
    jobtitle = Column(String(256), nullable=True, comment="Job title")
    phone = Column(String(64), nullable=True, comment="Phone number")
    lifecyclestage = Column(String(64), nullable=True, comment="e.g. lead, customer, opportunity")
    lead_status = Column(String(64), nullable=True, comment="hs_lead_status from HubSpot")

    # ── HubSpot system timestamps (stored as raw strings from API) ────────────
    createdate = Column(String(32), nullable=True)
    lastmodifieddate = Column(String(32), nullable=True)

    __table_args__ = (
        Index("ix_contacts_owner", "hubspot_owner_id"),
    )

    def __repr__(self) -> str:
        return f"<Contact id={self.id} email={self.email!r}>"