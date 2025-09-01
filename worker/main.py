import asyncio
import httpx
import logging
import time
import uuid
import threading

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger("worker")

CENTRAL_SERVER_URL = "http://127.0.0.1:8000" # This will need to be configurable
WORKER_ID = f"worker_{uuid.uuid4()}"
HEARTBEAT_INTERVAL = 30  # seconds
POLL_INTERVAL = 5 # seconds

# --- API Client Functions ---
async def register_with_server():
    """Register this worker with the central server."""
    log.info(f"Registering worker {WORKER_ID} with server at {CENTRAL_SERVER_URL}")
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{CENTRAL_SERVER_URL}/workers/register", json={"id": WORKER_ID})
            response.raise_for_status()
            log.info(f"Worker {WORKER_ID} registered successfully.")
            return True
        except httpx.RequestError as e:
            log.error(f"Failed to register with server: {e}")
            return False

def heartbeat_loop():
    """Continuously send heartbeats to the central server in a separate thread."""
    while True:
        try:
            time.sleep(HEARTBEAT_INTERVAL)
            log.info("Sending heartbeat...")
            response = httpx.post(f"{CENTRAL_SERVER_URL}/workers/{WORKER_ID}/heartbeat")
            response.raise_for_status()
            log.info("Heartbeat successful.")
        except httpx.RequestError as e:
            log.warning(f"Failed to send heartbeat: {e}")
        except Exception as e:
            log.error(f"An unexpected error occurred in the heartbeat loop: {e}")


async def get_next_task():
    """Poll the central server for the next available task."""
    log.info("Polling for next task...")
    async with httpx.AsyncClient(timeout=POLL_INTERVAL + 5) as client:
        try:
            response = await client.get(f"{CENTRAL_SERVER_URL}/tasks/next", params={"worker_id": WORKER_ID})
            if response.status_code == 404:
                log.info("No pending tasks available.")
                return None
            response.raise_for_status()
            task_data = response.json()
            log.info(f"Received task: {task_data['infohash']}")
            return task_data
        except httpx.RequestError as e:
            log.error(f"Failed to get next task: {e}")
            return None

import os
import glob
from screenshot.service import ScreenshotService
from screenshot.config import Settings

# --- API Client Functions (add update_task_status and upload_screenshot) ---

async def update_task_status(infohash: str, status: str, resume_data: dict = None):
    """Report the status of a task back to the central server."""
    log.info(f"Updating task {infohash} to status {status}")
    payload = {"status": status, "resume_data": resume_data}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.put(f"{CENTRAL_SERVER_URL}/tasks/{infohash}/status", json=payload)
            response.raise_for_status()
            log.info(f"Successfully updated status for {infohash}.")
        except httpx.RequestError as e:
            log.error(f"Failed to update status for {infohash}: {e}")

async def upload_screenshot(infohash: str, filepath: str):
    """Upload a single screenshot file to the server."""
    filename = os.path.basename(filepath)
    # Extract timestamp from filename like 'INFO_HASH_HH-MM-SS.jpg'
    try:
        timestamp = filename.split('_')[1].replace('.jpg', '')
    except IndexError:
        timestamp = "00-00-00"

    log.info(f"Uploading screenshot {filename} for task {infohash}")
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            with open(filepath, "rb") as f:
                files = {'file': (filename, f, 'image/jpeg')}
                response = await client.post(f"{CENTRAL_SERVER_URL}/tasks/{infohash}/screenshot?timestamp={timestamp}", files=files)
                response.raise_for_status()
            log.info(f"Successfully uploaded {filename}.")
            return True
        except httpx.RequestError as e:
            log.error(f"Failed to upload {filename}: {e}")
            return False

# --- Screenshot Service Integration ---

async def upload_and_cleanup_screenshots(infohash: str, output_dir: str):
    """Find, upload, and delete screenshots for a completed task."""
    log.info(f"Scanning {output_dir} for screenshots for task {infohash}...")
    screenshot_files = glob.glob(os.path.join(output_dir, f"{infohash}_*.jpg"))

    if not screenshot_files:
        log.warning(f"No screenshot files found for successful task {infohash}.")
        return

    upload_tasks = [upload_screenshot(infohash, f) for f in screenshot_files]
    results = await asyncio.gather(*upload_tasks)

    for i, filepath in enumerate(screenshot_files):
        if results[i]: # If upload was successful
            try:
                os.remove(filepath)
                log.info(f"Cleaned up {filepath}")
            except OSError as e:
                log.error(f"Failed to clean up {filepath}: {e}")

async def status_callback(status: str, infohash: str, message: str, error=None, resume_data=None):
    """Callback function for ScreenshotService to report final status."""
    log.info(f"Task {infohash} finished with status: {status}. Message: {message}")

    if status == 'success':
        # If successful, first upload the generated images
        await upload_and_cleanup_screenshots(infohash, Settings().output_dir)

    # Finally, update the server with the final status
    await update_task_status(infohash, status, resume_data)


# --- Main Worker Logic ---

async def process_task(task: dict):
    """Handle the execution of a single screenshot task."""
    infohash = task['infohash']
    resume_data = task.get('resume_data')
    log.info(f"Processing task {infohash}...")

    try:
        settings = Settings()
        # Ensure the output directory exists
        os.makedirs(settings.output_dir, exist_ok=True)

        service = ScreenshotService(settings=settings, status_callback=status_callback)

        # We run the service's core logic directly, not its long-running `run` method.
        # This requires creating a temporary client and managing its lifecycle.
        await service.client.start()
        await service._handle_screenshot_task({'infohash': infohash, 'resume_data': resume_data})
        await service.client.stop()

    except Exception as e:
        log.exception(f"An unexpected exception occurred while processing task {infohash}.")
        await update_task_status(infohash, "permanent_failure")

async def main_loop():
    """The main loop for the worker."""
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()

    while True:
        task = await get_next_task()
        if task:
            await process_task(task)
        else:
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    log.info("Starting worker...")
    # First, register with the server. If it fails, we exit.
    if not asyncio.run(register_with_server()):
        exit(1)

    # Start the main processing loop
    asyncio.run(main_loop())
