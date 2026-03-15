from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


connect_args = {"check_same_thread": False}
engine = create_engine("sqlite:///./app.db", connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
