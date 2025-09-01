import asyncio
import logging
import uuid
import time
import requests
from typing import Optional, Dict

# Basic configuration
SERVER_URL = "http://127.0.0.1:8000" # The address of the FastAPI server
HEARTBEAT_INTERVAL = 30  # seconds
POLL_INTERVAL = 5       # seconds

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("Worker")


class APIClient:
    """A simple client to interact with the server's REST API."""

    def __init__(self, server_url: str, worker_id: str):
        self.server_url = server_url
        self.worker_id = worker_id
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def register(self) -> bool:
        """Registers the worker with the server."""
        url = f"{self.server_url}/api/workers/register"
        try:
            response = self.session.post(url, json={"worker_id": self.worker_id})
            response.raise_for_status()
            log.info(f"Successfully registered with server. Server response: {response.json()}")
            return True
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to register with server: {e}")
            return False

    def send_heartbeat(self, status: str = "idle"):
        """Sends a heartbeat to the server."""
        url = f"{self.server_url}/api/workers/{self.worker_id}/heartbeat"
        try:
            params = {"status": status}
            response = self.session.post(url, params=params)
            response.raise_for_status()
            log.debug(f"Heartbeat sent with status '{status}'.")
        except requests.exceptions.RequestException as e:
            log.warning(f"Failed to send heartbeat: {e}")

    def get_next_task(self) -> Optional[Dict]:
        """Polls the server for the next available task."""
        url = f"{self.server_url}/api/tasks/next"
        try:
            params = {"worker_id": self.worker_id}
            response = self.session.get(url, params=params)
            response.raise_for_status()
            if response.status_code == 200 and response.json():
                return response.json()
            return None
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to get next task: {e}")
            return None

    def report_task_complete(self, task_id: int, zip_file_path: str):
        """Reports a task as complete and uploads the result."""
        url = f"{self.server_url}/api/tasks/{task_id}/complete"
        try:
            with open(zip_file_path, 'rb') as f:
                files = {'result': (os.path.basename(zip_file_path), f, 'application/zip')}
                response = self.session.post(url, files=files)
                response.raise_for_status()
            log.info(f"[Task {task_id}] Successfully reported completion to server.")
        except requests.exceptions.RequestException as e:
            log.error(f"[Task {task_id}] Failed to report completion to server: {e}")

    def report_task_fail(self, task_id: int, status: str, error_message: str, resume_data: Optional[Dict] = None):
        """Reports a task as failed."""
        url = f"{self.server_url}/api/tasks/{task_id}/fail"
        payload = {
            "status": status,
            "error_message": error_message,
            "resume_data": resume_data
        }
        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            log.info(f"[Task {task_id}] Successfully reported failure to server.")
        except requests.exceptions.RequestException as e:
            log.error(f"[Task {task_id}] Failed to report failure to server: {e}")

# Import runner after APIClient is fully defined
from . import runner
import os

# Global state to track worker status
worker_status = "idle"

async def heartbeat_loop(client: APIClient):
    """A loop that runs in the background to send periodic heartbeats."""
    global worker_status
    while True:
        client.send_heartbeat(status=worker_status)
        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def main():
    """The main entry point for the worker."""
    global worker_status
    worker_id = f"worker-{uuid.uuid4()}"
    log.info(f"Starting worker with ID: {worker_id}")

    api_client = APIClient(server_url=SERVER_URL, worker_id=worker_id)

    # Attempt to register with the server
    if not api_client.register():
        log.error("Could not register with server. Exiting.")
        return

    # Start the background heartbeat task
    asyncio.create_task(heartbeat_loop(api_client))
    log.info(f"Heartbeat service started. Will send heartbeat every {HEARTBEAT_INTERVAL} seconds.")

    log.info("Starting main loop to poll for tasks...")
    while True:
        try:
            if worker_status == "idle":
                task = api_client.get_next_task()
                if task:
                    worker_status = "busy"
                    log.info(f"Received new task: {task['id']}")
                    await runner.run_task(api_client, task)
                    log.info(f"Finished processing task {task['id']}. Returning to polling.")
                    worker_status = "idle"
                else:
                    # No task, wait before polling again
                    await asyncio.sleep(POLL_INTERVAL)
            else:
                # Still busy, wait before checking status again
                await asyncio.sleep(POLL_INTERVAL)

        except Exception as e:
            log.exception(f"An error occurred in the main loop: {e}")
            worker_status = "idle" # Reset status on error
            # Wait a bit longer after an error before retrying
            await asyncio.sleep(POLL_INTERVAL * 2)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Worker shutting down.")
