from sqlalchemy.orm import Session
from . import models, schemas
import datetime

# --- Worker CRUD ---

def get_worker(db: Session, worker_id: str):
    return db.query(models.Worker).filter(models.Worker.id == worker_id).first()

def create_worker(db: Session, worker: schemas.WorkerCreate):
    db_worker = models.Worker(id=worker.id, status="idle", last_seen=datetime.datetime.utcnow())
    db.add(db_worker)
    db.commit()
    db.refresh(db_worker)
    return db_worker

def update_worker_heartbeat(db: Session, worker_id: str):
    db_worker = get_worker(db, worker_id)
    if db_worker:
        db_worker.last_seen = datetime.datetime.utcnow()
        db.commit()
        db.refresh(db_worker)
    return db_worker

# --- Task CRUD ---

def get_task(db: Session, infohash: str):
    return db.query(models.Task).filter(models.Task.infohash == infohash).first()

def get_tasks(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Task).offset(skip).limit(limit).all()

def create_task(db: Session, task: schemas.TaskCreate):
    # Don't create a new task if one with the same infohash already exists.
    db_task = get_task(db, infohash=task.infohash)
    if db_task:
        return db_task

    db_task = models.Task(infohash=task.infohash, status="pending")
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task

def update_task_status(db: Session, infohash: str, status_update: schemas.TaskUpdate):
    db_task = get_task(db, infohash)
    if db_task:
        db_task.status = status_update.status
        if status_update.resume_data:
            db_task.resume_data = status_update.resume_data
        if status_update.worker_id:
            db_task.worker_id = status_update.worker_id
        # If the task is no longer being worked on, clear the worker_id
        if status_update.status in ["success", "permanent_failure", "recoverable_failure", "pending"]:
             db_task.worker_id = None

        db.commit()
        db.refresh(db_task)
    return db_task

def get_next_pending_task(db: Session):
    # Lock the selected row to prevent other workers from picking up the same task.
    return db.query(models.Task).filter(models.Task.status == "pending").with_for_update().first()

# --- Screenshot CRUD ---

def create_screenshot(db: Session, screenshot: schemas.ScreenshotCreate, infohash: str):
    db_screenshot = models.Screenshot(**screenshot.dict(), task_infohash=infohash)
    db.add(db_screenshot)
    db.commit()
    db.refresh(db_screenshot)
    return db_screenshot
