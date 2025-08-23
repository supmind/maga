import asyncio
import signal
import logging

from screenshot.service import ScreenshotService

# Configure logging to see the output from the service
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# 设置服务日志级别为 DEBUG，以输出更详细的信息
logging.getLogger("ScreenshotService").setLevel(logging.DEBUG)


async def main():
    loop = asyncio.get_running_loop()

    # Create and run the screenshot service
    service = ScreenshotService(loop=loop)
    await service.run()

    # --- Example Tasks ---
    # Submit a few screenshot tasks to the service.
    # The user should replace these with real infohashes and desired timestamps.
    tasks_to_submit = [

        # Sintel - another open-source movie torrent
        ("08ada5a7a6183aae1e09d831df6748d566095a10", "00:05:00")
    ]

    for infohash, timestamp in tasks_to_submit:
        await service.submit_task(infohash, timestamp)

    print("\nScreenshot service is running.")
    print(f"Submitted {len(tasks_to_submit)} example tasks. The service will now process them.")
    print("Screenshots will be saved in the 'screenshots/' directory.")
    print("Press Ctrl+C to stop the service.")

    # Wait for graceful shutdown or timeout
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)

    timeout = 300  # seconds
    print(f"Running for a maximum of {timeout} seconds. Will stop automatically.")

    try:
        await asyncio.wait_for(stop, timeout=timeout)
    except asyncio.TimeoutError:
        print(f"\nTimeout reached after {timeout} seconds.")

    # Clean up
    print("\nStopping service...")
    service.stop()
    print("Service stopped.")


if __name__ == "__main__":
    asyncio.run(main())
