from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import datetime

# --- Worker Schemas ---

class WorkerBase(BaseModel):
    worker_id: str

class WorkerCreate(WorkerBase):
    pass

class WorkerInfo(WorkerBase):
    id: int
    status: str
    last_heartbeat: datetime.datetime
    created_at: datetime.datetime

    class Config:
        # This allows the Pydantic model to be created from an ORM model
        # (i.e., you can do `WorkerInfo.from_orm(db_worker)`)
        from_attributes = True

# --- Task Schemas ---

class TaskBase(BaseModel):
    infohash: str
    task_metadata: Optional[Dict[str, Any]] = None

class TaskCreate(TaskBase):
    pass

class TaskUpdate(BaseModel):
    status: Optional[str] = None
    error_message: Optional[str] = None
    resume_data: Optional[Dict[str, Any]] = None
    result_path: Optional[str] = None

class TaskInfo(TaskBase):
    id: int
    status: str
    resume_data: Optional[Dict[str, Any]] = None
    result_path: Optional[str] = None
    error_message: Optional[str] = None
    worker_id: Optional[int] = None
    created_at: datetime.datetime
    updated_at: datetime.datetime

    class Config:
        from_attributes = True

class TaskAssign(TaskInfo):
    """Schema for the data sent to a worker when a task is assigned."""
    pass
