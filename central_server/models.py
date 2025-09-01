import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Worker(Base):
    __tablename__ = "workers"
    id = Column(String, primary_key=True, index=True)
    last_seen = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    status = Column(String, default="idle")

    tasks = relationship("Task", back_populates="worker")

class Task(Base):
    __tablename__ = "tasks"
    infohash = Column(String, primary_key=True, index=True)
    status = Column(String, default="pending", index=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    worker_id = Column(String, ForeignKey("workers.id"), nullable=True)
    worker = relationship("Worker", back_populates="tasks")

    resume_data = Column(JSON, nullable=True)

    screenshots = relationship("Screenshot", back_populates="task", cascade="all, delete-orphan")
    failed_frames = relationship("FailedFrame", back_populates="task", cascade="all, delete-orphan")

class Screenshot(Base):
    __tablename__ = "screenshots"
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_infohash = Column(String, ForeignKey("tasks.infohash"), nullable=False)
    filename = Column(String, nullable=False)
    timestamp = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    task = relationship("Task", back_populates="screenshots")

class FailedFrame(Base):
    __tablename__ = "failed_frames"
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_infohash = Column(String, ForeignKey("tasks.infohash"), nullable=False)
    frame_identifier = Column(String, nullable=False)
    reason = Column(Text, nullable=True)

    task = relationship("Task", back_populates="failed_frames")
