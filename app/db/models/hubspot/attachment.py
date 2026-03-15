from sqlalchemy import Column, Integer, String, UniqueConstraint

from app.db.base import Base


class Attachment(Base):
    __tablename__ = "attachments"

    id = Column(Integer, primary_key=True, index=True)
    deal_id = Column(String, nullable=False, index=True)
    note_id = Column(String, nullable=False)
    file_id = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("deal_id", "note_id", "file_id", name="uq_attachment"),
    )
