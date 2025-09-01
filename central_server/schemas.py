from pydantic import BaseModel
import datetime
from typing import List, Optional, Any

# --- Screenshot Schemas ---
class ScreenshotBase(BaseModel):
    filename: str
    timestamp: Optional[str] = None

class ScreenshotCreate(ScreenshotBase):
    pass

class Screenshot(ScreenshotBase):
    id: int
    task_infohash: str
    created_at: datetime.datetime

    class Config:
        orm_mode = True

# --- FailedFrame Schemas ---
class FailedFrameBase(BaseModel):
    frame_identifier: str
    reason: Optional[str] = None

class FailedFrameCreate(FailedFrameBase):
    pass

class FailedFrame(FailedFrameBase):
    id: int
    task_infohash: str

    class Config:
        orm_mode = True

# --- Worker Schemas ---
class WorkerBase(BaseModel):
    id: str
    status: Optional[str] = "idle"

class WorkerCreate(WorkerBase):
    pass

class Worker(WorkerBase):
    last_seen: datetime.datetime

    class Config:
        orm_mode = True

# --- Task Schemas ---
class TaskBase(BaseModel):
    infohash: str

class TaskCreate(TaskBase):
    pass

class TaskUpdate(BaseModel):
    status: str
    resume_data: Optional[dict] = None
    worker_id: Optional[str] = None

class Task(TaskBase):
    status: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    worker_id: Optional[str] = None
    resume_data: Optional[dict] = None
    screenshots: List[Screenshot] = []
    failed_frames: List[FailedFrame] = []

    class Config:
        orm_mode = True
