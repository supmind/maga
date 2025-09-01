import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship

from .database import Base

class Worker(Base):
    __tablename__ = "workers"

    id = Column(Integer, primary_key=True, index=True)
    worker_id = Column(String, unique=True, index=True, nullable=False)
    status = Column(String, default="disconnected", nullable=False) # e.g., idle, busy, disconnected
    last_heartbeat = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    tasks = relationship("Task", back_populates="worker")


class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)
    infohash = Column(String, index=True, nullable=False)

    # Statuses: pending, in_progress, completed, permanent_failure, recoverable_failure
    status = Column(String, default="pending", index=True, nullable=False)

    # Using sqlalchemy.JSON which is backend-agnostic.
    # For PostgreSQL, this would ideally be JSONB.
    # Renamed from 'metadata' to 'task_metadata' to avoid conflicts with the SQLAlchemy reserved keyword.
    task_metadata = Column(JSON, nullable=True)
    resume_data = Column(JSON, nullable=True)

    result_path = Column(String, nullable=True) # Path to the resulting .zip file
    error_message = Column(String, nullable=True)

    worker_id = Column(Integer, ForeignKey("workers.id"), nullable=True)
    worker = relationship("Worker", back_populates="tasks")

    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)
