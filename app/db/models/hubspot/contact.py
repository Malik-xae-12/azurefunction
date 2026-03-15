from sqlalchemy import Column, String

from app.db.base import Base


class Contact(Base):
    __tablename__ = "contacts"

    id = Column(String, primary_key=True)
    hs_object_id = Column(String, nullable=True, index=True)
    email = Column(String, nullable=True)
    firstname = Column(String, nullable=True)
    lastname = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    createdate = Column(String, nullable=True)
    lastmodifieddate = Column(String, nullable=True)
