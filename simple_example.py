import asyncio
import binascii
import logging
import signal
import collections
import argparse

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
    discards the oldest item. This prevents unbounded memory growth.
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


# Use a BoundedSet to keep track of infohashes to prevent memory leaks.
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

            # The BoundedSet `add` method returns False if the item already exists.
            # We use it here to de-duplicate tasks from the queue and to mark
            # an infohash as "seen" before we attempt to download it.
            if not PROCESSED_INFOHASHES.add(infohash_hex):
                task_queue.task_done()
                continue

            log.info(f"Processing infohash: {infohash_hex} from peer {peer_addr}")

            # Asynchronously download metadata from the announcing peer
            loop = asyncio.get_running_loop()
            info = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop, timeout=2)

            if info:
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
            return
        except Exception:
            log.exception("Error in metadata_downloader worker.")
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
            self.task_queue.put_nowait((infohash, peer_addr))
        except asyncio.QueueFull:
            pass # Drop the task if the queue is full


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


async def main(args):
    """
    The main entry point for the producer-consumer based crawler.
    """
    log.info("Starting the advanced DHT crawler (Producer-Consumer Model)...")
    loop = asyncio.get_running_loop()

    # A bounded queue to hold tasks, which acts as a buffer.
    task_queue = asyncio.Queue(maxsize=2000)

    # The crawler acts as the producer
    crawler = SimpleCrawler(task_queue=task_queue)

    # The number of workers determines the download concurrency
    num_workers = args.workers
    workers = [
        loop.create_task(metadata_downloader(task_queue))
        for _ in range(num_workers)
    ]

    # Run the crawler on the specified port
    await crawler.run(port=args.port)
    log.info(f"Crawler is running on port {crawler.transport.get_extra_info('sockname')[1]}")

    # Start the periodic statistics printer
    stats_task = loop.create_task(print_stats(crawler, task_queue))

    log.info(f"{num_workers} download workers started. Press Ctrl+C to stop.")

    # Handle graceful shutdown
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    log.info("Shutting down...")
    stats_task.cancel()
    for worker in workers:
        worker.cancel()
    crawler.stop()
    await asyncio.gather(*workers, return_exceptions=True)
    await task_queue.join()
    log.info("Crawler shut down gracefully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A simple, robust DHT crawler.")
    parser.add_argument("--port", type=int, default=6881, help="Port to listen on for DHT traffic.")
    parser.add_argument("--workers", type=int, default=200, help="Number of concurrent metadata download workers.")
    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        log.info("Crawler stopped by user.")
