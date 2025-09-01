from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from .. import crud, schemas, database
import shutil
import os

router = APIRouter(
    prefix="/tasks",
    tags=["Tasks"],
)

UPLOADS_DIR = "./uploads"

@router.post("/", response_model=schemas.Task)
def create_task(task: schemas.TaskCreate, db: Session = Depends(database.get_db)):
    """
    Create a new screenshot task.
    If a task with the same infohash already exists, it will not be duplicated.
    """
    db_task = crud.get_task(db, infohash=task.infohash)
    if db_task:
        # If task exists, just return it.
        return db_task
    return crud.create_task(db=db, task=task)

@router.get("/next", response_model=schemas.Task)
def get_next_task(worker_id: str, db: Session = Depends(database.get_db)):
    """
    Get the next available task from the queue for a worker.
    This will atomically find a 'pending' task and assign it to the worker.
    """
    db_task = crud.get_next_pending_task(db)
    if db_task is None:
        raise HTTPException(status_code=404, detail="No pending tasks available")

    # Assign the task to the worker
    update = schemas.TaskUpdate(status="working", worker_id=worker_id)
    return crud.update_task_status(db, infohash=db_task.infohash, status_update=update)

@router.get("/{infohash}", response_model=schemas.Task)
def get_task_status(infohash: str, db: Session = Depends(database.get_db)):
    """
    Get the status of a specific task.
    """
    db_task = crud.get_task(db, infohash=infohash)
    if db_task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return db_task

@router.put("/{infohash}/status", response_model=schemas.Task)
def update_task_status(infohash: str, status: schemas.TaskUpdate, db: Session = Depends(database.get_db)):
    """
    Update the status of a task. Used by workers to report progress.
    """
    db_task = crud.get_task(db, infohash=infohash)
    if db_task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return crud.update_task_status(db, infohash=infohash, status_update=status)

@router.post("/{infohash}/screenshot", response_model=schemas.Screenshot)
def upload_screenshot(infohash: str, timestamp: str, file: UploadFile = File(...), db: Session = Depends(database.get_db)):
    """
    Upload a generated screenshot image for a task.
    """
    db_task = crud.get_task(db, infohash=infohash)
    if db_task is None:
        raise HTTPException(status_code=404, detail="Task not found for this screenshot")

    # Ensure the upload directory exists
    os.makedirs(UPLOADS_DIR, exist_ok=True)

    # Sanitize filename
    safe_filename = f"{infohash}_{timestamp.replace(':', '-')}.jpg"
    file_path = os.path.join(UPLOADS_DIR, safe_filename)

    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    finally:
        file.file.close()

    screenshot_data = schemas.ScreenshotCreate(filename=safe_filename, timestamp=timestamp)
    return crud.create_screenshot(db=db, screenshot=screenshot_data, infohash=infohash)
