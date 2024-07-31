"""Defines connections with database"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# SQLALCHEMY_DATABASE_URL = "postgresql://mywebuser:Password@localhost:5432/company"
SQLALCHEMY_DATABASE_URL = "postgresql://mywebuser:HsSU5oVz57wYdzB9yw8j3nraoYpv3jh2@dpg-cql04rt6l47c73f0ha90-a.oregon-postgres.render.com/company_pvjs"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    """Creates local session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
