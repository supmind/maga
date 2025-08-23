import asyncio
import signal
import logging

from screenshot.service import ScreenshotService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Set our service's logger to DEBUG to see more details if needed
logging.getLogger("ScreenshotService").setLevel(logging.DEBUG)


async def main():
    loop = asyncio.get_running_loop()

    # Create and run the screenshot service
    service = ScreenshotService(loop=loop)
    await service.run()

    # --- Example Tasks ---
    # Submit a few screenshot tasks to the service
    # The user should replace these with real infohashes
    tasks_to_submit = [
        # Big Buck Bunny - for a known, well-seeded, web-optimized MP4
        ("bbb6a578d5583a241484439b0ee576203104c615", "00:01:00"),
        # A second task
        ("a84a761166c37617b07040a33118c72834b6b803", "00:00:10")
    ]

    for infohash, timestamp in tasks_to_submit:
        await service.submit_task(infohash, timestamp)

    print("\nScreenshot service is running.")
    print("Submitted example tasks. The service will now process them.")
    print("Screenshots will be saved in the 'screenshots/' directory.")
    print("Press Ctrl+C to stop the service.")

    # Wait for graceful shutdown
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    # Clean up
    print("\nStopping service...")
    service.stop()
    print("Service stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
