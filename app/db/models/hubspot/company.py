from sqlalchemy import Column, String

from app.db.base import Base


class Company(Base):
    __tablename__ = "companies"

    id = Column(String, primary_key=True)
    hs_object_id = Column(String, nullable=True, index=True)
    name = Column(String, nullable=True)
    domain = Column(String, nullable=True)
    industry = Column(String, nullable=True)
    city = Column(String, nullable=True)
    createdate = Column(String, nullable=True)
    hs_lastmodifieddate = Column(String, nullable=True)
