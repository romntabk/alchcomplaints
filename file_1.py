from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_utils import database_exists, create_database


Base = declarative_base()
class Complaint(Base):
    __tablename__ = 'complaints'
    complaint_id=Column(Integer,primary_key=True)
    date_received=Column(DateTime(),nullable=True)
    date_sent_to_company=Column(DateTime(),nullable=True)
    state=Column(String(50),nullable=True)
    consumer_disputed=Column(String(50),nullable=True)
    timely=Column(String(50),nullable=True)
    company_response=Column(String(150),nullable=True)
    submitted_via=Column(String(50),nullable=True)
    consumer_consent_provided=Column(String(50),nullable=True)
    tags=Column(String(50),nullable=True)
    zip_code=Column(String(50),nullable=True)
    company=Column(String(150),nullable=True)
    company_public_response=Column(String(150),nullable=True)
    complaint_what_happened=Column(String(50000),nullable=True)
    issue=Column(String(150),nullable=True)
    sub_issue=Column(String(150),nullable=True)
    product=Column(String(150),nullable=True)
    sub_product=Column(String(150),nullable=True)
    
if __name__ == '__main__':
    engine = create_engine("postgresql+psycopg2://postgres:postpass@localhost/dbdb")
    session = Session(bind=engine)
    if not database_exists(engine.url):
        create_database(engine.url)
    Base.metadata.create_all(engine)