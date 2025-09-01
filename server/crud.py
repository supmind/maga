from sqlalchemy.orm import Session
import datetime

from . import models
from shared import schemas

# --- Worker CRUD ---

def get_worker_by_id(db: Session, worker_id: str):
    """
    Retrieve a worker by its unique string ID.
    """
    return db.query(models.Worker).filter(models.Worker.worker_id == worker_id).first()

def get_all_workers(db: Session, skip: int = 0, limit: int = 100):
    """
    Retrieve all workers.
    """
    return db.query(models.Worker).offset(skip).limit(limit).all()

def create_or_update_worker(db: Session, worker: schemas.WorkerCreate):
    """
    Creates a new worker or updates the last_heartbeat if it already exists.
    This is effectively a "register" or "upsert" operation.
    """
    db_worker = get_worker_by_id(db, worker_id=worker.worker_id)
    if db_worker:
        db_worker.last_heartbeat = datetime.datetime.utcnow()
        db_worker.status = "idle" # Assume worker is idle on registration
    else:
        db_worker = models.Worker(
            worker_id=worker.worker_id,
            status="idle",
            last_heartbeat=datetime.datetime.utcnow()
        )
        db.add(db_worker)
    db.commit()
    db.refresh(db_worker)
    return db_worker

def update_worker_heartbeat(db: Session, worker_id: str, status: str = "idle"):
    """
    Updates the heartbeat and status for a given worker.
    """
    db_worker = get_worker_by_id(db, worker_id=worker_id)
    if db_worker:
        db_worker.last_heartbeat = datetime.datetime.utcnow()
        db_worker.status = status
        db.commit()
        db.refresh(db_worker)
    return db_worker


# --- Task CRUD ---

def create_task(db: Session, task: schemas.TaskCreate):
    """
    Create a new task in the database.
    """
    db_task = models.Task(
        infohash=task.infohash,
        task_metadata=task.task_metadata,
        status="pending"
    )
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task

def get_task(db: Session, task_id: int):
    """
    Get a single task by its integer ID.
    """
    return db.query(models.Task).filter(models.Task.id == task_id).first()

def get_next_pending_task(db: Session):
    """
    Find the next available task with 'pending' status.
    """
    return db.query(models.Task).filter(models.Task.status == "pending").order_by(models.Task.created_at).first()

def assign_task_to_worker(db: Session, task: models.Task, worker: models.Worker):
    """
    Assigns a task to a worker and updates statuses.
    """
    task.worker_id = worker.id
    task.status = "in_progress"
    worker.status = "busy"
    db.commit()
    db.refresh(task)
    db.refresh(worker)
    return task

def update_task_status(db: Session, task: models.Task, status: str, error_message: str = None, result_path: str = None, resume_data: dict = None):
    """
    Updates the status and other fields of a task.
    """
    task.status = status
    task.error_message = error_message
    task.result_path = result_path
    task.resume_data = resume_data

    # If the task is finished, the worker is now idle
    if status in ["completed", "permanent_failure", "recoverable_failure"]:
        if task.worker:
            task.worker.status = "idle"

    db.commit()
    db.refresh(task)
    return task
