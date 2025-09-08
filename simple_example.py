import asyncio
import binascii
import logging
import signal
import collections

from maga.crawler import Maga
from maga.downloader import get_metadata

# Configure basic logging to see the output from the crawler and this script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


class BoundedSet:
    """
    A set with a fixed maximum size. When full, adding a new item
    discards the oldest item.
    """
    def __init__(self, max_size=1_000_000):
        self.max_size = max_size
        self.deque = collections.deque()
        self.set = set()

    def add(self, item):
        if item in self.set:
            return False

        if len(self.deque) == self.max_size:
            oldest = self.deque.popleft()
            self.set.remove(oldest)

        self.deque.append(item)
        self.set.add(item)
        return True

    def __contains__(self, item):
        return item in self.set


# Use a BoundedSet to keep track of infohashes we've already processed.
# This prevents memory from growing indefinitely.
PROCESSED_INFOHASHES = BoundedSet(max_size=1_000_000)


def format_bytes(size):
    """Formats a size in bytes into a human-readable string (KB, MB, GB)."""
    if size is None:
        return "N/A"
    power = 1024
    n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size >= power and n < len(power_labels) - 1:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}"


async def metadata_downloader(task_queue):
    """
    This is the "consumer" or "worker". It pulls tasks from the queue
    and downloads metadata.
    """
    while True:
        try:
            infohash, peer_addr = await task_queue.get()
            infohash_hex = binascii.hexlify(infohash).decode()

            # First, check if this infohash has been successfully processed already.
            if infohash_hex in PROCESSED_INFOHASHES:
                task_queue.task_done()
                continue

            log.info(f"Processing infohash: {infohash_hex} from peer {peer_addr}")

            # Asynchronously download metadata from the announcing peer
            loop = asyncio.get_running_loop()
            info = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop, timeout=10)

            if info:
                # Only add the infohash to the processed set on successful download.
                PROCESSED_INFOHASHES.add(infohash_hex)
                name = info.get(b'name', b'Unknown').decode(errors='ignore')
                if b'files' in info:
                    num_files = len(info[b'files'])
                    total_size = sum(f.get(b'length', 0) for f in info[b'files'])
                else:
                    num_files = 1
                    total_size = info.get(b'length', 0)

                log.info("=" * 30 + " METADATA DOWNLOADED " + "=" * 30)
                log.info(f"  Name: {name}")
                log.info(f"  Infohash: {infohash_hex}")
                log.info(f"  Size: {format_bytes(total_size)}")
                log.info(f"  Files: {num_files}")
                log.info("=" * 82 + "\n")

            # Notify the queue that this task is complete
            task_queue.task_done()
        except asyncio.CancelledError:
            # Propagate cancellation
            return
        except Exception:
            log.exception("Error in metadata_downloader worker.")
            # Still need to mark task as done even if it failed
            task_queue.task_done()


class SimpleCrawler(Maga):
    """
    This is the "producer". It discovers infohashes and puts them into
    the task queue.
    """
    def __init__(self, task_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_queue = task_queue

    async def handler(self, infohash, addr, peer_addr=None):
        """
        This handler is called for `announce_peer` messages.
        It puts the discovered task into the queue for the workers to process.
        """
        if not peer_addr:
            return

        try:
            # Don't block, if the queue is full, just drop the task
            self.task_queue.put_nowait((infohash, peer_addr))
        except asyncio.QueueFull:
            log.warning("Task queue is full, dropping new infohash.")

    def get_routing_table_stats(self):
        """
        Calculates and returns statistics about the DHT routing table.
        """
        total_nodes = sum(len(bucket) for bucket in self.k_buckets)
        non_empty_buckets = sum(1 for bucket in self.k_buckets if bucket)
        return {
            "total_nodes": total_nodes,
            "non_empty_buckets": non_empty_buckets
        }


async def print_stats(crawler, task_queue):
    """
    A periodic task to print statistics about the crawler and the task queue.
    """
    while True:
        await asyncio.sleep(30)
        stats = crawler.get_routing_table_stats()
        log.info(
            f"[STATS] DHT Nodes: {stats['total_nodes']} | "
            f"Queue Size: {task_queue.qsize()}/{task_queue.maxsize} | "
            f"Processed Hashes: {len(PROCESSED_INFOHASHES.deque)}"
        )


async def main():
    """
    The main entry point for the producer-consumer based crawler.
    """
    log.info("Starting the advanced DHT crawler (Producer-Consumer Model)...")
    loop = asyncio.get_running_loop()

    # Create a bounded queue to hold tasks
    # The size of this queue is a buffer between discovery and downloading.
    task_queue = asyncio.Queue(maxsize=1000)

    # Create the crawler (producer) and pass it the queue
    crawler = SimpleCrawler(task_queue=task_queue)

    # Create a pool of workers (consumers)
    # The number of workers is the concurrency limit for downloads.
    num_workers = 100
    workers = [
        loop.create_task(metadata_downloader(task_queue))
        for _ in range(num_workers)
    ]

    # Run the crawler
    await crawler.run()
    log.info(f"Crawler is running on port {crawler.transport.get_extra_info('sockname')[1]}")

    # Start the periodic statistics printer
    stats_task = loop.create_task(print_stats(crawler, task_queue))

    log.info(f"{num_workers} download workers started. Press Ctrl+C to stop.")

    # Set up signal handling for graceful shutdown
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    log.info("Shutting down...")
    # 1. Stop the crawler from accepting new connections
    crawler.stop()
    # 2. Cancel the stats printer
    stats_task.cancel()
    # 3. Cancel the worker tasks
    for worker in workers:
        worker.cancel()
    # 4. Wait for all workers to finish their cancellation
    await asyncio.gather(*workers, return_exceptions=True)
    log.info("All workers stopped.")
    # 5. Wait for the queue to be fully processed (optional, but good practice)
    await task_queue.join()
    log.info("Crawler shut down gracefully.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Crawler stopped by user.")
