# -*- coding: utf-8 -*-
"""
本模块包含了所有与数据库交互的 CRUD (Create, Read, Update, Delete) 操作函数。
这些函数封装了 SQLAlchemy 的查询逻辑，供 API 端点调用。
"""
import datetime
from sqlalchemy.orm import Session
from typing import Optional
from . import models, schemas

def get_task_by_infohash(db: Session, infohash: str) -> Optional[models.Task]:
    """
    根据 infohash 从数据库中检索任务。

    :param db: 数据库会话。
    :param infohash: 要查询的任务的 infohash。
    :return: 找到的 Task 对象，如果不存在则返回 None。
    """
    return db.query(models.Task).filter(models.Task.infohash == infohash).first()

def get_tasks(db: Session, status: Optional[str] = None, skip: int = 0, limit: int = 100) -> tuple[int, list[models.Task]]:
    """
    分页检索任务列表，可选择按状态过滤。

    :param db: 数据库会话。
    :param status: (可选) 要筛选的任务状态。
    :param skip: 要跳过的记录数。
    :param limit: 要返回的最大记录数。
    :return: 一个元组，包含任务总数和当前页的任务对象列表。
    """
    query = db.query(models.Task)
    if status:
        query = query.filter(models.Task.status == status)

    total = query.count()
    tasks = query.order_by(models.Task.created_at.desc()).offset(skip).limit(limit).all()
    return total, tasks

def create_task(db: Session, task: schemas.TaskCreate) -> models.Task:
    """
    在数据库中创建一个新任务。

    :param db: 数据库会话。
    :param task: 包含新任务信息的 Pydantic schema 对象。
    :return: 新创建的 Task 对象。
    """
    db_task = models.Task(infohash=task.infohash)
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task


def upsert_worker(db: Session, worker_id: str, status: str) -> models.Worker:
    """
    原子性地插入或更新一个工作节点。
    如果工作节点已存在，则更新其状态和最后心跳时间；如果不存在，则创建一个新记录。
    这对于处理工作节点注册和心跳的竞争条件至关重要。

    :param db: 数据库会话。
    :param worker_id: 工作节点的唯一标识符。
    :param status: 工作节点的当前状态。
    :return: 创建或更新后的 Worker 对象。
    """
    db_worker = db.query(models.Worker).filter(models.Worker.worker_id == worker_id).with_for_update().first()
    if db_worker:
        db_worker.status = status
        db_worker.last_seen_at = datetime.datetime.utcnow()
    else:
        db_worker = models.Worker(
            worker_id=worker_id,
            status=status,
            last_seen_at=datetime.datetime.utcnow()
        )
        db.add(db_worker)
    db.commit()
    db.refresh(db_worker)
    return db_worker

def reset_stuck_tasks(db: Session, timeout_seconds: int) -> int:
    """
    重置长时间处于 'working' 状态的任务。
    如果一个任务处于 'working' 状态，但其 `updated_at` 时间戳早于指定的超时时间，
    则将其状态重置为 'pending'，以便其他工作节点可以接手。

    :param db: 数据库会话。
    :param timeout_seconds: 定义任务被视为“卡住”的秒数。
    :return: 被重置的任务数量。
    """
    timeout_threshold = datetime.datetime.utcnow() - datetime.timedelta(seconds=timeout_seconds)

    # 注意： `update()` 方法是原子性的，并且比先查询再逐个更新要高效得多
    updated_count = db.query(models.Task).filter(
        models.Task.status == 'working',
        models.Task.updated_at < timeout_threshold
    ).update({
        'status': 'pending',
        'assigned_worker_id': None
    }, synchronize_session=False)

    db.commit()
    return updated_count


def get_worker_by_id(db: Session, worker_id: str) -> Optional[models.Worker]:
    """
    根据 worker_id 从数据库中检索工作节点。

    :param db: 数据库会话。
    :param worker_id: 要查询的工作节点的ID。
    :return: 找到的 Worker 对象，如果不存在则返回 None。
    """
    return db.query(models.Worker).filter(models.Worker.worker_id == worker_id).first()

def create_worker(db: Session, worker: schemas.WorkerCreate) -> models.Worker:
    """
    在数据库中注册一个新的工作节点。

    :param db: 数据库会话。
    :param worker: 包含新工作节点信息的 Pydantic schema 对象。
    :return: 新创建的 Worker 对象。
    """
    db_worker = models.Worker(worker_id=worker.worker_id, status=worker.status)
    db.add(db_worker)
    db.commit()
    db.refresh(db_worker)
    return db_worker

def get_workers(db: Session, skip: int = 0, limit: int = 100) -> tuple[int, list[models.Worker]]:
    """
    分页检索工作节点列表。

    :param db: 数据库会话。
    :param skip: 要跳过的记录数。
    :param limit: 要返回的最大记录数。
    :return: 一个元组，包含工作节点总数和当前页的工作节点对象列表。
    """
    total = db.query(models.Worker).count()
    workers = db.query(models.Worker).order_by(models.Worker.created_at.desc()).offset(skip).limit(limit).all()
    return total, workers

def get_pending_tasks_count(db: Session) -> int:
    """
    计算状态为 'pending' 的任务总数。

    :param db: 数据库会话。
    :return: 待处理任务的数量。
    """
    return db.query(models.Task).filter(models.Task.status == 'pending').count()


def update_worker_status(db: Session, worker_id: str, status: str, active_tasks_count: int, queue_size: int) -> Optional[models.Worker]:
    """
    更新一个工作节点的状态和最后心跳时间。

    :param db: 数据库会话。
    :param worker_id: 要更新的工作节点的ID。
    :param status: 工作节点的新状态。
    :param active_tasks_count: 工作节点正在执行的任务数。
    :param queue_size: 工作节点内部的队列大小。
    :return: 更新后的 Worker 对象，如果工作节点不存在则返回 None。
    """
    db_worker = get_worker_by_id(db, worker_id=worker_id)
    if db_worker:
        db_worker.last_seen_at = datetime.datetime.utcnow()
        db_worker.status = status
        db_worker.active_tasks_count = active_tasks_count
        db_worker.queue_size = queue_size
        db.commit()
        db.refresh(db_worker)
    return db_worker

def get_and_assign_next_task(db: Session, worker_id: str) -> Optional[models.Task]:
    """
    原子性地获取下一个待处理任务，并将其分配给指定的工作节点。
    使用悲观锁 (SELECT ... FOR UPDATE) 来防止多个工作节点获取到同一个任务的竞态条件。

    :param db: 数据库会话。
    :param worker_id: 请求任务的工作节点的ID。
    :return: 被分配的任务对象，如果没有待处理任务则返回 None。
    """
    # .with_for_update() 是实现行级锁的关键
    db_task = db.query(models.Task).filter(models.Task.status == 'pending').order_by(models.Task.created_at).with_for_update().first()

    if db_task:
        db_task.status = 'working'
        db_task.assigned_worker_id = worker_id
        db.commit()
        db.refresh(db_task)

    return db_task

def update_task_status(
    db: Session, infohash: str, status: str, message: Optional[str] = None
) -> Optional[models.Task]:
    """
    更新任务的最终状态和结果消息。
    当任务完成（成功或永久失败）或进入可恢复失败状态时，会清除分配的工作节点ID。

    :param db: 数据库会话。
    :param infohash: 要更新的任务的 infohash。
    :param status: 任务的新状态。
    :param message: 相关的结果消息。
    :return: 更新后的 Task 对象，如果任务不存在则返回 None。
    """
    db_task = get_task_by_infohash(db, infohash=infohash)
    if db_task:
        db_task.status = status
        db_task.result_message = message

        # 当任务进入最终状态或可恢复失败状态时，它不再被任何工作节点持有
        if status in ['success', 'permanent_failure', 'recoverable_failure']:
            db_task.assigned_worker_id = None

        db.commit()
        db.refresh(db_task)
    return db_task


def update_task_details(db: Session, infohash: str, details: schemas.TaskDetailsUpdate) -> Optional[models.Task]:
    """
    更新任务的详细信息，如 torrent 名称、视频文件名和时长。
    这个函数会忽略值为 None 的字段，只更新被提供值的字段。

    :param db: 数据库会话。
    :param infohash: 要更新的任务的 infohash。
    :param details: 包含要更新的详细信息的 Pydantic schema 对象。
    :return: 更新后的 Task 对象，如果任务不存在则返回 None。
    """
    db_task = get_task_by_infohash(db, infohash=infohash)
    if db_task:
        # 使用 model_dump 替代已废弃的 dict，并排除未设置的字段
        update_data = details.model_dump(exclude_unset=True)

        # 显式地更新每个字段，以提高代码的清晰度和健壮性
        if "torrent_name" in update_data:
            db_task.torrent_name = update_data["torrent_name"]
        if "video_filename" in update_data:
            db_task.video_filename = update_data["video_filename"]
        if "video_duration_seconds" in update_data:
            db_task.video_duration_seconds = update_data["video_duration_seconds"]

        db.commit()
        db.refresh(db_task)
    return db_task

def record_screenshot(db: Session, infohash: str, filename: str) -> Optional[models.Task]:
    """
    将一个成功生成的截图文件名附加到任务的 `successful_screenshots` JSON 数组中。
    此操作是并发安全的。通过对任务行施加 `SELECT ... FOR UPDATE` 悲观锁，
    保证了“读取-修改-写入”整个过程的原子性，能有效防止多个工作节点并发上传截图时
    发生竞态条件导致的数据丢失。

    :param db: 数据库会话。
    :param infohash: 截图所属任务的 infohash。
    :param filename: 要记录的截图文件名。
    :return: 更新后的 Task 对象，如果任务不存在则返回 None。
    """
    # 使用 with_for_update() 对任务行施加悲观锁。
    # 这确保了从读取 successful_screenshots 字段到更新它的整个过程是原子的，
    # 可以防止多个并发请求同时修改该字段时导致的数据丢失问题。
    db_task = db.query(models.Task).filter(models.Task.infohash == infohash).with_for_update().first()

    if db_task:
        # 如果字段为 None，则初始化为一个空列表
        current_screenshots = list(db_task.successful_screenshots) if db_task.successful_screenshots else []

        if filename not in current_screenshots:
            current_screenshots.append(filename)
            db_task.successful_screenshots = current_screenshots

        # 提交事务后，锁会自动释放
        db.commit()
        db.refresh(db_task)

    return db_task
