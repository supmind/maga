import asyncio
import logging
import datetime
from sqlalchemy.orm import Session

from . import crud, models
from .database import SessionLocal

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("BackgroundTask")

# --- Configuration for the background task ---
WORKER_TIMEOUT_SECONDS = 60 * 10  # 10 minutes
TASK_TIMEOUT_SECONDS = 60 * 30    # 30 minutes
SLEEP_INTERVAL_SECONDS = 60 * 5   # 5 minutes

def cleanup_stale_workers_and_tasks():
    """
    The core cleanup logic. This function is designed to be run periodically.
    It finds and handles disconnected workers and stale tasks.
    """
    log.info("Running background cleanup task...")
    db: Session = SessionLocal()
    try:
        # 1. Find stale workers
        stale_threshold = datetime.datetime.utcnow() - datetime.timedelta(seconds=WORKER_TIMEOUT_SECONDS)
        stale_workers = db.query(models.Worker).filter(
            models.Worker.last_heartbeat < stale_threshold,
            models.Worker.status != "disconnected"
        ).all()

        if stale_workers:
            log.info(f"Found {len(stale_workers)} stale workers to mark as disconnected.")
            for worker in stale_workers:
                log.warning(f"Worker '{worker.worker_id}' timed out. Last heartbeat: {worker.last_heartbeat}.")
                worker.status = "disconnected"

                # Re-queue tasks assigned to this worker
                stale_tasks = db.query(models.Task).filter(
                    models.Task.worker_id == worker.id,
                    models.Task.status == "in_progress"
                ).all()
                if stale_tasks:
                    log.info(f"Re-queuing {len(stale_tasks)} tasks from disconnected worker '{worker.worker_id}'.")
                    for task in stale_tasks:
                        task.status = "pending"
                        task.worker_id = None
                        task.error_message = "Task re-queued due to worker timeout."
            db.commit()

        # 2. Find stale tasks (that might be stuck 'in_progress' for any reason)
        task_timeout_threshold = datetime.datetime.utcnow() - datetime.timedelta(seconds=TASK_TIMEOUT_SECONDS)
        stale_tasks = db.query(models.Task).filter(
            models.Task.status == "in_progress",
            models.Task.updated_at < task_timeout_threshold
        ).all()

        if stale_tasks:
            log.info(f"Found {len(stale_tasks)} stale tasks that exceeded the global timeout.")
            for task in stale_tasks:
                log.warning(f"Task ID {task.id} (infohash: {task.infohash}) timed out. Last update: {task.updated_at}.")
                task.status = "pending"
                if task.worker:
                    task.worker.status = "idle" # The worker might still be alive, just stuck on this task
                task.worker_id = None
                task.error_message = "Task re-queued due to exceeding max processing time."
            db.commit()

        log.info("Background cleanup task finished.")
    finally:
        db.close()


async def periodic_cleanup_task():
    """
    An async wrapper that runs the cleanup logic periodically.
    """
    log.info("Starting periodic cleanup background task.")
    while True:
        try:
            cleanup_stale_workers_and_tasks()
        except Exception:
            log.exception("An error occurred during the background cleanup task.")

        await asyncio.sleep(SLEEP_INTERVAL_SECONDS)
