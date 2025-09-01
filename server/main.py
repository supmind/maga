import asyncio
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from typing import List, Optional

from . import crud, models, database, background
from shared import schemas

# Create all database tables based on the models
# This is simple for bring-up, but for production, a migration tool like Alembic is recommended.
models.Base.metadata.create_all(bind=database.engine)

app = FastAPI(
    title="Distributed Screenshot Service",
    description="The central server for managing screenshot tasks and workers.",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    """
    On application startup, create a background task for periodic cleanup.
    """
    app.state.cleanup_task = asyncio.create_task(background.periodic_cleanup_task())

@app.on_event("shutdown")
async def shutdown_event():
    """
    On application shutdown, cancel the background task.
    """
    app.state.cleanup_task.cancel()

# --- API Endpoints ---

# A simple root endpoint to check if the server is running
@app.get("/")
def read_root():
    return {"message": "Screenshot Service server is running."}

# --- Worker Management Endpoints ---

@app.post("/api/workers/register", response_model=schemas.WorkerInfo, status_code=201)
def register_worker(worker: schemas.WorkerCreate, db: Session = Depends(database.get_db)):
    """
    Endpoint for a new worker to register itself with the server.
    If the worker already exists, its heartbeat is updated.
    """
    db_worker = crud.get_worker_by_id(db, worker_id=worker.worker_id)
    if db_worker:
        # If worker exists, just update its heartbeat and set status to idle
        return crud.update_worker_heartbeat(db=db, worker_id=worker.worker_id, status="idle")

    # If worker does not exist, create it
    return crud.create_or_update_worker(db=db, worker=worker)

@app.post("/api/workers/{worker_id}/heartbeat", response_model=schemas.WorkerInfo)
def worker_heartbeat(worker_id: str, status: str = "idle", db: Session = Depends(database.get_db)):
    """
    Endpoint for a worker to send a heartbeat, updating its status and last_seen time.
    A worker should send 'busy' when it's processing a task.
    """
    db_worker = crud.update_worker_heartbeat(db=db, worker_id=worker_id, status=status)
    if db_worker is None:
        raise HTTPException(status_code=404, detail=f"Worker with ID '{worker_id}' not found.")
    return db_worker

@app.get("/api/workers", response_model=List[schemas.WorkerInfo])
def list_workers(skip: int = 0, limit: int = 100, db: Session = Depends(database.get_db)):
    """
    Get a list of all registered workers and their current status.
    """
    workers = crud.get_all_workers(db, skip=skip, limit=limit)
    return workers

# --- Task Management Endpoints ---

@app.post("/api/tasks", response_model=schemas.TaskInfo, status_code=201)
def create_task(task: schemas.TaskCreate, db: Session = Depends(database.get_db)):
    """
    Create a new screenshot task. The task will be queued for processing by a worker.
    """
    return crud.create_task(db=db, task=task)

@app.get("/api/tasks/next", response_model=Optional[schemas.TaskAssign])
def get_next_task(worker_id: str, db: Session = Depends(database.get_db)):
    """
    Called by a worker to request the next available task.
    This endpoint implements the core task distribution logic.
    """
    worker = crud.get_worker_by_id(db, worker_id=worker_id)
    if not worker:
        raise HTTPException(status_code=404, detail=f"Worker with ID '{worker_id}' not registered.")

    if worker.status == "busy":
        # Worker is already busy, so don't assign a new task.
        # This can happen in race conditions.
        return None

    # Find the next pending task
    task = crud.get_next_pending_task(db)

    if task:
        # Assign the task to this worker
        assigned_task = crud.assign_task_to_worker(db, task=task, worker=worker)
        return assigned_task

    # No pending tasks
    return None

@app.get("/api/tasks/{task_id}", response_model=schemas.TaskInfo)
def get_task_status(task_id: int, db: Session = Depends(database.get_db)):
    """
    Retrieve the status and details of a specific task.
    """
    db_task = crud.get_task(db, task_id=task_id)
    if db_task is None:
        raise HTTPException(status_code=404, detail=f"Task with ID {task_id} not found.")
    return db_task

@app.post("/api/tasks/{task_id}/complete", response_model=schemas.TaskInfo)
async def task_complete(task_id: int, result: UploadFile = File(...), db: Session = Depends(database.get_db)):
    """
    Called by a worker to mark a task as complete and upload the resulting file.
    """
    db_task = crud.get_task(db, task_id=task_id)
    if not db_task:
        raise HTTPException(status_code=404, detail=f"Task with ID {task_id} not found.")

    # In a real application, you would save this file to a persistent storage
    # like S3, a local file server, etc. For now, we'll just store a dummy path.
    # The file content can be accessed with `await result.read()`.
    file_path = f"results/{db_task.infohash}_{task_id}.zip"

    # Here you would add the logic to save the file:
    # os.makedirs("results", exist_ok=True)
    # with open(file_path, "wb") as buffer:
    #     buffer.write(await result.read())

    return crud.update_task_status(db=db, task=db_task, status="completed", result_path=file_path)

@app.post("/api/tasks/{task_id}/fail", response_model=schemas.TaskInfo)
def task_fail(task_id: int, failure_data: schemas.TaskUpdate, db: Session = Depends(database.get_db)):
    """
    Called by a worker to report a task failure. The failure can be permanent
    or recoverable (in which case resume_data should be provided).
    """
    db_task = crud.get_task(db, task_id=task_id)
    if not db_task:
        raise HTTPException(status_code=404, detail=f"Task with ID {task_id} not found.")

    # Determine the status, default to permanent_failure if not specified
    status = failure_data.status or "permanent_failure"
    if status not in ["permanent_failure", "recoverable_failure"]:
        raise HTTPException(status_code=400, detail="Invalid failure status. Must be 'permanent_failure' or 'recoverable_failure'.")

    return crud.update_task_status(
        db=db,
        task=db_task,
        status=status,
        error_message=failure_data.error_message,
        resume_data=failure_data.resume_data
    )
