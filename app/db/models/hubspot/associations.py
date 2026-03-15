from sqlalchemy import Column, String

from app.db.base import Base


class DealContact(Base):
    __tablename__ = "deal_contacts"

    deal_id = Column(String, primary_key=True)
    contact_id = Column(String, primary_key=True)


class DealCompany(Base):
    __tablename__ = "deal_companies"

    deal_id = Column(String, primary_key=True)
    company_id = Column(String, primary_key=True)
