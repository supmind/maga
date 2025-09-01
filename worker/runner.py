import asyncio
import logging
import os
import shutil
import tempfile
import zipfile
from typing import Dict

# To allow the runner to find the 'screenshot' module, we need to add its parent directory to the path.
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from screenshot.service import ScreenshotService
from screenshot.config import Settings
from .main import APIClient

log = logging.getLogger("TaskRunner")

async def run_task(api_client: APIClient, task_data: Dict):
    """
    Runs a single screenshot task using the existing ScreenshotService.

    This function orchestrates:
    1. Setting up a temporary directory for output.
    2. Creating a custom status callback to report results to the server.
    3. Running the ScreenshotService.
    4. Zipping and uploading the results upon success.
    """
    infohash = task_data['infohash']
    task_id = task_data['id']
    resume_data = task_data.get('resume_data')

    log.info(f"[Task {task_id}] Starting processing for infohash: {infohash}")

    # 1. Create a temporary directory for this task's output
    temp_output_dir = tempfile.mkdtemp(prefix=f"task_{task_id}_")
    log.info(f"[Task {task_id}] Created temporary output directory: {temp_output_dir}")

    # 2. Define the custom status callback
    task_result = asyncio.Future()

    async def server_status_callback(status: str, infohash: str, message: str, **kwargs):
        """This callback sends the result back to the central server via the API."""
        log.info(f"[Task {task_id}] Status update: {status.upper()} - {message}")
        if status == 'success':
            task_result.set_result({'status': 'completed', 'message': message})
        elif status == 'recoverable_failure':
            resume_data = kwargs.get('resume_data')
            result = {'status': 'recoverable_failure', 'message': message, 'resume_data': resume_data}
            task_result.set_result(result)
        elif status == 'permanent_failure':
            result = {'status': 'permanent_failure', 'message': message}
            task_result.set_result(result)

    try:
        # 3. Configure and run the ScreenshotService
        settings = Settings(output_dir=temp_output_dir)
        loop = asyncio.get_running_loop()

        service = ScreenshotService(
            settings=settings,
            loop=loop,
            status_callback=server_status_callback
        )

        # Run the service components but not the worker pool
        await service.run()

        # Submit the single task we received from the server
        await service.submit_task(infohash, resume_data=resume_data)

        # Wait for the task to be processed and the callback to set the future's result.
        # Add a timeout to prevent it from running forever.
        final_result = await asyncio.wait_for(task_result, timeout=settings.piece_queue_timeout + 60)

        await service.stop()

        # 4. Process the final result
        if final_result['status'] == 'completed':
            log.info(f"[Task {task_id}] Task completed successfully. Zipping results...")
            # Create a zip file of the screenshots
            zip_path = os.path.join(temp_output_dir, f"{infohash}.zip")
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for root, _, files in os.walk(temp_output_dir):
                    for file in files:
                        if file.endswith('.jpg'):
                            zipf.write(os.path.join(root, file), arcname=file)

            log.info(f"[Task {task_id}] Uploading zip file: {zip_path}")
            api_client.report_task_complete(task_id, zip_path)

        else: # Handle failure cases
            log.warning(f"[Task {task_id}] Task failed with status: {final_result['status']}")
            api_client.report_task_fail(
                task_id=task_id,
                status=final_result['status'],
                error_message=final_result['message'],
                resume_data=final_result.get('resume_data')
            )

    except asyncio.TimeoutError:
        log.error(f"[Task {task_id}] Task processing timed out.")
        api_client.report_task_fail(task_id, 'permanent_failure', "Task processing timed out in worker.")
    except Exception as e:
        log.exception(f"[Task {task_id}] An unexpected error occurred while running the task.")
        api_client.report_task_fail(task_id, 'permanent_failure', f"An unexpected error occurred: {e}")
    finally:
        # Clean up the temporary directory
        log.info(f"[Task {task_id}] Cleaning up temporary directory: {temp_output_dir}")
        shutil.rmtree(temp_output_dir)
