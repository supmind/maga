import asyncio
import signal
import binascii

from maga.crawler import Maga
from maga.downloader import Downloader

# A set to keep track of infohashes we've already submitted to the downloader
# This is to prevent the crawler from flooding the download queue with the same
# infohash if it's announced frequently.
SUBMITTED_INFOHASHES = set()


async def main():
    loop = asyncio.get_running_loop()

    # 1. Create the Downloader service
    # This service runs its own DHT node and a pool of workers
    downloader = Downloader(loop=loop, dht_port=6882, num_workers=10)

    # 2. Define the handler for the Crawler
    # This function will be called by the crawler when it finds an infohash.
    # Its job is to submit the infohash to the downloader's queue.
    async def crawler_handler(infohash, peer_addr):
        infohash_hex = binascii.hexlify(infohash).decode()
        if infohash_hex not in SUBMITTED_INFOHASHES:
            SUBMITTED_INFOHASHES.add(infohash_hex)
            print(f"[Crawler] Discovered new infohash: {infohash_hex}. Submitting to downloader.")
            await downloader.submit(infohash)

    # 3. Create the Crawler service
    # We pass our handler to it.
    crawler = Maga(loop=loop, handler=crawler_handler)

    # 4. Start both services
    # The Downloader runs its DHT node on port 6882
    await downloader.run()

    # The Crawler runs on port 6881
    await crawler.run(port=6881)

    print("\nCrawler and Downloader services are running.")
    print("Crawler listens for new torrents.")
    print("Downloader waits for tasks and downloads metadata.")
    print("Press Ctrl+C to stop.")

    # 5. Wait for graceful shutdown
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    # 6. Clean up
    print("\nStopping services...")
    downloader.stop()
    crawler.stop()
    print("Services stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
