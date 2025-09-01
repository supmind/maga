import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# --- Absolute Path for Database ---
# Using /tmp as it's a universally writable location, to rule out
# any and all permission/pathing issues.
SQLALCHEMY_DATABASE_URL = "sqlite:////tmp/screenshot_service.db"

# The engine is the entry point to the database.
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    # `connect_args` is needed only for SQLite to allow multi-threaded access.
    connect_args={"check_same_thread": False}
)

# Each instance of the SessionLocal class will be a database session.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# This Base will be used to create each of the database models (the ORM models).
Base = declarative_base()

def get_db():
    """
    Dependency to get a DB session for each request.
    Ensures the session is always closed after the request.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
