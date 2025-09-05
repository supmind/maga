# -*- coding: utf-8 -*-
"""
本模块定义了 API 端点使用的所有 Pydantic 模型 (schemas)。
这些模型用于数据验证、序列化和 API 文档生成。
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Union, Dict, Any
import datetime

# --- 任务 (Task) 相关的模型 ---

class TaskBase(BaseModel):
    infohash: str = Field(..., max_length=40, description="任务的唯一标识，即 Infohash。")

class TaskCreate(TaskBase):
    pass

class Task(TaskBase):
    id: int
    status: str = Field("pending", description="任务的当前状态 (e.g., pending, working, success, failure)。")
    torrent_name: Optional[str] = None
    video_filename: Optional[str] = None
    video_duration_seconds: Optional[int] = None
    created_at: datetime.datetime
    assigned_worker_id: Optional[str] = None
    result_message: Optional[str] = None
    successful_screenshots: Optional[List[str]] = Field([], description="成功生成的截图文件名列表。")

    class Config:
        orm_mode = True

class TaskList(BaseModel):
    total: int
    tasks: List[Task]

class NextTaskResponse(BaseModel):
    infohash: str
    metadata: Optional[str] = None # Base64-encoded
    resume_data: Optional[Dict[str, Any]] = None

class TaskStatusUpdate(BaseModel):
    status: str
    message: Optional[str] = None
    resume_data: Optional[Dict[str, Any]] = None


class TaskDetailsUpdate(BaseModel):
    torrent_name: Optional[str] = None
    video_filename: Optional[str] = None
    video_duration_seconds: Optional[int] = None


class ScreenshotRecord(BaseModel):
    filename: str = Field(..., description="要记录的截图文件名。")


# --- 工作节点 (Worker) 相关的模型 ---

class WorkerBase(BaseModel):
    worker_id: str

class WorkerCreate(WorkerBase):
    status: str = "idle"

class WorkerHeartbeat(WorkerBase):
    status: str
    active_tasks_count: int
    queue_size: int

class Worker(WorkerBase):
    id: int
    status: str
    created_at: datetime.datetime
    last_seen_at: datetime.datetime
    active_tasks_count: int
    queue_size: int

    class Config:
        orm_mode = True

class WorkerList(BaseModel):
    total: int
    workers: List[Worker]
