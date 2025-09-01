from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Use SQLite for simplicity. The database will be a single file in /tmp.
SQLALCHEMY_DATABASE_URL = "sqlite:////tmp/server.db"

# The 'check_same_thread' argument is needed only for SQLite.
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

# Each instance of SessionLocal will be a database session.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for our models.
Base = declarative_base()

# Dependency to get a DB session for each request.
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_db_and_tables():
    # This function will create all tables in the database.
    # It's typically called once at application startup.
    from .models import Base
    Base.metadata.create_all(bind=engine)
