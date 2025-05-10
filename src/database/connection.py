import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

from ..utils.logger import logger
from ..config import DATABASE_URL

# create SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800, # recycling connections after 30 minutes.
    poolclass=QueuePool
)

# Create session factory.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for SQLAlchemy models.
Base = declarative_base()

def get_db_session():
    """Get a databse session"""
    session = SessionLocal()
    try:
        return session
    except Exception as e:
        logger.error(f"Database session error: {e}")
        session.close()
        raise
    
def init_db():
    """Initialize the database (create tables)"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise