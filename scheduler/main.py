# -*- coding: utf-8 -*-
"""
主应用程序文件，包含了所有 FastAPI 的 API 端点。
这是分布式截图服务的调度器组件。
"""
from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File, Form, Response, Query, BackgroundTasks
from sqlalchemy.orm import Session
import datetime
from typing import Optional
import os
import shutil
import base64
import logging
import asyncio
import json

from . import crud, models, schemas
from .database import SessionLocal, engine
from .security import get_api_key

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- 数据库初始化 ---
# 在应用启动时，根据定义的模型创建所有数据库表。
models.Base.metadata.create_all(bind=engine)

# --- FastAPI 应用实例 ---
app = FastAPI(
    title="分布式截图调度器",
    description="用于管理和分发截图任务的中心调度服务。",
    version="0.1.0",
)

# --- 依赖项 ---
def get_db():
    """
    一个 FastAPI 依赖项，用于在每个请求的生命周期内提供数据库会话。
    它确保数据库会话在使用后总是被关闭。
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- 后台任务 ---
async def reset_stuck_tasks_periodically(db_session_factory, timeout: int, interval: int):
    """
    一个后台任务，定期检查并重置卡死的任务。
    :param db_session_factory: 用于创建新的数据库会话的函数。
    :param timeout: 任务被视为卡死的秒数。
    :param interval: 检查周期的秒数。
    """
    while True:
        await asyncio.sleep(interval)
        db = db_session_factory()
        try:
            log.info("开始执行后台任务：重置卡死的任务...")
            reset_count = crud.reset_stuck_tasks(db, timeout_seconds=timeout)
            if reset_count > 0:
                log.info(f"成功重置了 {reset_count} 个卡死的任务。")
            else:
                log.info("没有发现卡死的任务。")
        finally:
            db.close()

# --- 应用启动事件 ---
@app.on_event("startup")
async def startup_event():
    """
    在应用启动时，启动一个后台任务来处理卡死的任务。
    """
    log.info("应用启动，开始后台任务...")
    # 从环境变量或配置中获取超时和间隔时间，这里为了简单直接硬编码
    task_timeout_seconds = 300  # 5 分钟
    check_interval_seconds = 60   # 1 分钟
    asyncio.create_task(reset_stuck_tasks_periodically(SessionLocal, task_timeout_seconds, check_interval_seconds))

# --- API 端点 ---

@app.get("/", tags=["通用"])
def read_root():
    """一个简单的健康检查端点，用于确认服务正在运行。"""
    return {"message": "截图调度器正在运行"}

@app.post("/tasks/", response_model=schemas.Task, status_code=status.HTTP_200_OK, tags=["任务管理"])
async def create_or_reactivate_task(
    response: Response,
    infohash: str = Form(..., max_length=40, description="任务的 Infohash"),
    torrent_file: Optional[UploadFile] = File(None, description="可选的 .torrent 元数据文件"),
    db: Session = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    创建新任务或重新激活一个已存在的任务。
    - 如果任务已存在且状态为 pending, working, 或 success，则直接返回任务信息。
    - 如果任务为 permanent_failure，则拒绝请求。
    - 如果任务为 recoverable_failure，则将其状态重置为 pending 并返回。
    - 如果任务不存在，则创建新任务。
    """
    db_task = crud.get_task_by_infohash(db, infohash=infohash)
    if db_task:
        if db_task.status in ["pending", "working", "success"]:
            return db_task
        elif db_task.status == "permanent_failure":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务已永久失败，无法重新提交。")
        elif db_task.status == "recoverable_failure":
            db_task.status = "pending"
            db.commit()
            db.refresh(db_task)
            return db_task

    # 如果是新任务，设置响应状态码为 201 Created
    response.status_code = status.HTTP_201_CREATED
    if torrent_file:
        METADATA_DIR = "temp_metadata"
        os.makedirs(METADATA_DIR, exist_ok=True)
        file_path = os.path.join(METADATA_DIR, f"{infohash}.torrent")
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(torrent_file.file, buffer)

    new_task = crud.create_task(db=db, task=schemas.TaskCreate(infohash=infohash))
    return new_task

@app.get("/tasks/next", response_model=Optional[schemas.NextTaskResponse], tags=["任务管理"])
def get_next_task(worker_id: str = Query(..., description="请求任务的工作节点ID"), db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """
    为工作节点获取下一个待处理的任务。
    此端点会原子性地查询并分配一个 'pending' 状态的任务。
    如果任务有关联的恢复数据 (resume_data)，则会优先提供恢复数据。
    否则，如果存在元数据文件，则提供元数据文件。
    """
    METADATA_DIR = "temp_metadata"
    task = crud.get_and_assign_next_task(db, worker_id=worker_id)
    if not task:
        return None  # 返回 204 No Content

    response_data = {"infohash": task.infohash}

    # 优先检查并附加恢复数据（如果存在）
    RESUME_DIR = "resume_data"
    resume_file_path = os.path.join(RESUME_DIR, f"{task.infohash}.resume")
    if os.path.exists(resume_file_path):
        try:
            with open(resume_file_path, "r") as f:
                resume_data = json.load(f)
            response_data["resume_data"] = resume_data
            os.remove(resume_file_path) # 恢复数据是一次性的，使用后即删除
            log.info("在任务 %s 中发现并加载了恢复数据文件。", task.infohash)
        except (IOError, json.JSONDecodeError) as e:
            log.error(f"为任务 {task.infohash} 读取或删除恢复数据文件时失败: {e}")

    # 如果没有恢复数据，再检查并附加元数据（如果存在）
    if "resume_data" not in response_data:
        METADATA_DIR = "temp_metadata"
        metadata_path = os.path.join(METADATA_DIR, f"{task.infohash}.torrent")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as f:
                metadata_bytes = f.read()
                response_data["metadata"] = base64.b64encode(metadata_bytes).decode('ascii')

    return schemas.NextTaskResponse(**response_data)

@app.get("/tasks/{infohash}", response_model=schemas.Task, tags=["任务管理"])
def read_task(infohash: str, db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """根据 Infohash 查询特定任务的状态和详情。"""
    db_task = crud.get_task_by_infohash(db, infohash=infohash)
    if db_task is None:
        raise HTTPException(status_code=404, detail="任务未找到")
    return db_task

@app.get("/tasks/all/", response_model=schemas.TaskList, tags=["任务管理"])
def list_all_tasks(
    status: Optional[str] = Query(None, description="按任务状态筛选 (e.g., pending, working, success)"),
    skip: int = Query(0, ge=0, description="分页查询的起始位置"),
    limit: int = Query(100, ge=1, le=500, description="每页返回的任务数量"),
    db: Session = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    分页列出系统中的所有任务，可选择按状态进行筛选。
    """
    total, tasks = crud.get_tasks(db, status=status, skip=skip, limit=limit)
    return {"total": total, "tasks": tasks}

@app.post("/workers/register", response_model=schemas.Worker, tags=["工作节点管理"])
def register_worker(worker: schemas.WorkerCreate, db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """
    注册一个新的工作节点或更新一个已存在节点的状态。
    此端点是幂等的。如果工作节点已存在，则更新其状态和最后心跳时间；
    如果不存在，则创建一个新的记录。
    """
    return crud.upsert_worker(db=db, worker_id=worker.worker_id, status=worker.status)

@app.get("/tasks/queue/size", response_model=int, tags=["任务管理"])
def get_queue_size(db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """获取当前待处理任务队列的大小。"""
    return crud.get_pending_tasks_count(db)

@app.get("/workers/", response_model=schemas.WorkerList, tags=["工作节点管理"])
def list_all_workers(
    skip: int = Query(0, ge=0, description="分页查询的起始位置"),
    limit: int = Query(100, ge=1, le=500, description="每页返回的工作节点数量"),
    db: Session = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """分页列出所有已注册的工作节点。"""
    total, workers = crud.get_workers(db, skip=skip, limit=limit)
    return {"total": total, "workers": workers}


@app.post("/workers/heartbeat", response_model=schemas.Worker, tags=["工作节点管理"])
def worker_heartbeat(heartbeat: schemas.WorkerHeartbeat, db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """接收工作节点的心跳，更新其状态和最后在线时间。"""
    db_worker = crud.update_worker_status(
        db,
        worker_id=heartbeat.worker_id,
        status=heartbeat.status,
        active_tasks_count=heartbeat.active_tasks_count,
        queue_size=heartbeat.queue_size
    )
    if db_worker is None:
        raise HTTPException(status_code=404, detail="工作节点未找到，请先注册。")
    return db_worker


@app.post("/tasks/{infohash}/screenshots", response_model=schemas.Task, tags=["任务管理"])
async def record_screenshot_endpoint(infohash: str, record: schemas.ScreenshotRecord, db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """
    记录一个已成功上传到对象存储的截图文件名。
    """
    db_task = crud.record_screenshot(db, infohash=infohash, filename=record.filename)
    if db_task is None:
        raise HTTPException(status_code=404, detail="未找到与此截图关联的任务。")
    return db_task


@app.post("/tasks/{infohash}/status", response_model=schemas.Task, tags=["任务管理"])
async def update_task_status_endpoint(infohash: str, update: schemas.TaskStatusUpdate, db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """
    更新一个任务的最终状态。
    如果提供了 resume_data，它将被保存为文件，用于未来的任务恢复。
    """
    # 如果 worker 发送了 resume_data，则将其保存到文件系统
    if update.resume_data:
        RESUME_DIR = "resume_data"
        os.makedirs(RESUME_DIR, exist_ok=True)
        file_path = os.path.join(RESUME_DIR, f"{infohash}.resume")
        try:
            with open(file_path, "w") as f:
                json.dump(update.resume_data, f)
            log.info(f"已为任务 {infohash} 保存恢复数据到 {file_path}")
        except (IOError, TypeError) as e:
            log.error(f"为任务 {infohash} 保存恢复数据时失败: {e}")
            # 即使保存失败，也继续更新任务状态，但不抛出500错误
            # 因为状态更新更重要
            pass

    # 如果任务成功完成，则清理相关的元数据文件
    if update.status == 'success':
        METADATA_DIR = "temp_metadata"
        metadata_path = os.path.join(METADATA_DIR, f"{infohash}.torrent")
        if os.path.exists(metadata_path):
            try:
                os.remove(metadata_path)
                log.info(f"任务 {infohash} 成功，已清理元数据文件: {metadata_path}")
            except OSError as e:
                log.error(f"清理元数据文件 {metadata_path} 时失败: {e}")

    try:
        db_task = crud.update_task_status(
            db,
            infohash=infohash,
            status=update.status,
            message=update.message,
        )
        if db_task is None:
            raise HTTPException(status_code=404, detail="任务未找到")
        return db_task
    except Exception as e:
        log.error("更新任务 %s 状态时发生数据库错误: %s", infohash, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"更新任务状态时发生内部错误。")


@app.post("/tasks/{infohash}/details", response_model=schemas.Task, tags=["任务管理"])
async def update_task_details_endpoint(infohash: str, details: schemas.TaskDetailsUpdate, db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """
    更新任务的元数据详情，如种子名称、视频文件名和时长。
    此端点由工作节点在解析任务后调用。
    """
    db_task = crud.update_task_details(db, infohash=infohash, details=details)
    if db_task is None:
        raise HTTPException(status_code=404, detail="任务未找到")
    return db_task
