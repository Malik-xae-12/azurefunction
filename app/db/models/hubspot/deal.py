from sqlalchemy import Column, String
from app.db.base import Base

class Deal(Base):
    __tablename__ = "deals"

    id = Column(String, primary_key=True)
    hs_object_id = Column(String, nullable=True, index=True)
    dealname = Column(String, nullable=True)
    amount = Column(String, nullable=True)
    dealstage = Column(String, nullable=True)
    dealstage_label = Column(String, nullable=True)
    closedate = Column(String, nullable=True)
    createdate = Column(String, nullable=True)
    hs_lastmodifieddate = Column(String, nullable=True)
    hubspot_owner_id = Column(String, nullable=True)
    pipeline = Column(String, nullable=True)
    deal_owner_email = Column(String, nullable=True)
    delivery_owner = Column(String, nullable=True)
    project_start_date = Column(String, nullable=True)
    project_end_date = Column(String, nullable=True)
    po_hours = Column(String, nullable=True)



#deal type,deal description