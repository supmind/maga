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
# Set the service log level to INFO to reduce verbosity
logging.getLogger("ScreenshotService").setLevel(logging.INFO)


async def main():
    loop = asyncio.get_running_loop()

    # Create and run the screenshot service
    # Screenshots will be saved to the default directory: './screenshots_output'
    # To specify a different directory, use:
    # service = ScreenshotService(loop=loop, output_dir='/path/to/your/dir')
    service = ScreenshotService(loop=loop)
    await service.run()

    # --- Example Tasks ---
    # Submit a few screenshot tasks to the service.
    # The user should replace these with real infohashes and desired timestamps.
    # Sintel - an open-source movie torrent
    infohash_to_submit = "08ada5a7a6183aae1e09d831df6748d566095a10"

    await service.submit_task(infohash_to_submit)

    print("\nScreenshot service is running.")
    print("Submitted 1 example task. The service will now process it.")
    print(f"Screenshots will be saved in the '{service.output_dir}' directory.")
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
