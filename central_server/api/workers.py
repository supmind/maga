from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from .. import crud, schemas, database

router = APIRouter(
    prefix="/workers",
    tags=["Workers"],
)

@router.post("/register", response_model=schemas.Worker)
def register_worker(worker: schemas.WorkerCreate, db: Session = Depends(database.get_db)):
    """
    Register a new worker with the system.
    If a worker with the same ID already exists, it will be returned.
    """
    db_worker = crud.get_worker(db, worker_id=worker.id)
    if db_worker:
        # Worker trying to re-register, just update its heartbeat and return it
        return crud.update_worker_heartbeat(db, worker_id=db_worker.id)
    return crud.create_worker(db=db, worker=worker)

@router.post("/{worker_id}/heartbeat", response_model=schemas.Worker)
def worker_heartbeat(worker_id: str, db: Session = Depends(database.get_db)):
    """
    A worker calls this endpoint periodically to signal that it is still alive.
    """
    db_worker = crud.get_worker(db, worker_id=worker_id)
    if db_worker is None:
        raise HTTPException(status_code=404, detail="Worker not found")
    return crud.update_worker_heartbeat(db=db, worker_id=worker_id)
