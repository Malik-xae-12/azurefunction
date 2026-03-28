"""Company ORM model — columns match HubSpot Create Company form exactly. No soft-delete columns."""

from sqlalchemy import Column, Index, String, Text

from app.db.base import Base


class Company(Base):
    __tablename__ = "companies"

    # ── Primary key ───────────────────────────────────────────────────────────
    id = Column(String(64), primary_key=True, comment="HubSpot company ID (hs_object_id)")
    hs_object_id = Column(String(64), nullable=True, index=True)

    # ── Fields matching HubSpot Create Company form (top to bottom) ───────────
    domain = Column(String(256), nullable=True, index=True, comment="Company domain name")
    name = Column(String(512), nullable=True, comment="Company name")
    hubspot_owner_id = Column(String(64), nullable=True, comment="Company owner — HubSpot user ID")
    industry = Column(String(256), nullable=True)
    company_type = Column(String(128), nullable=True, comment="HubSpot 'Type' field e.g. PROSPECT, PARTNER")
    city = Column(String(128), nullable=True)
    state = Column(String(128), nullable=True, comment="State / Region")
    postal_code = Column(String(32), nullable=True, comment="Postal code / ZIP")
    number_of_employees = Column(String(32), nullable=True)
    annual_revenue = Column(String(64), nullable=True)
    timezone = Column(String(64), nullable=True)
    description = Column(Text, nullable=True)
    linkedin_company_page = Column(String(512), nullable=True)

    # ── HubSpot system timestamps (stored as raw strings from API) ────────────
    createdate = Column(String(32), nullable=True)
    hs_lastmodifieddate = Column(String(32), nullable=True)

    __table_args__ = (
        Index("ix_companies_owner", "hubspot_owner_id"),
    )

    def __repr__(self) -> str:
        return f"<Company id={self.id} name={self.name!r} domain={self.domain!r}>"