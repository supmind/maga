import asyncio
import binascii
import logging
import signal
from collections import deque

from maga.crawler import Maga
from maga.downloader import get_metadata

# Configure basic logging to see the output from the crawler and this script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


# A simple set to keep track of infohashes we've already processed in this session
# This helps avoid downloading the same metadata multiple times
PROCESSED_INFOHASHES = set()


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


class SimpleCrawler(Maga):
    """
    A simple crawler that demonstrates core functionality.
    """
    async def handler(self, infohash, addr, peer_addr=None):
        """
        This is the main handler for discovered infohashes.
        It's called by `handle_get_peers` and `handle_announce_peer` in the base class.
        """
        infohash_hex = binascii.hexlify(infohash).decode()

        # Ignore if we've already processed this infohash
        if infohash_hex in PROCESSED_INFOHASHES:
            return

        log.info(f"Discovered new infohash: {infohash_hex} from {addr}")
        PROCESSED_INFOHASHES.add(infohash_hex)

        # The peer address is necessary to download metadata
        # In a get_peers query, we don't have a specific peer, just the DHT node.
        # In an announce_peer query, we get the peer_addr.
        target_addr = peer_addr or addr

        # Asynchronously download metadata
        # The loop object is fetched from the running event loop
        loop = asyncio.get_running_loop()
        info = await get_metadata(infohash, target_addr[0], target_addr[1], loop=loop)

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


async def print_stats(crawler):
    """
    A periodic task to print statistics about the crawler.
    """
    while True:
        await asyncio.sleep(30)  # Print stats every 30 seconds
        stats = crawler.get_routing_table_stats()
        log.info(
            f"[STATS] DHT Routing Table: "
            f"{stats['total_nodes']} nodes in {stats['non_empty_buckets']} buckets. "
            f"Processed {len(PROCESSED_INFOHASHES)} infohashes this session."
        )


async def main():
    """
    The main entry point for the simple crawler example.
    """
    log.info("Starting the simple DHT crawler...")
    loop = asyncio.get_running_loop()

    # Create an instance of our crawler
    crawler = SimpleCrawler()

    # Run the crawler. This will start listening on a UDP port.
    # We use a random available port by not specifying one.
    await crawler.run()
    log.info(f"Crawler is running on port {crawler.transport.get_extra_info('sockname')[1]}")

    # Start the periodic statistics printer task
    stats_task = loop.create_task(print_stats(crawler))

    log.info("Crawler started. Press Ctrl+C to stop.")

    # Set up signal handling for graceful shutdown
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    log.info("Shutting down the crawler...")
    stats_task.cancel()
    crawler.stop()
    log.info("Crawler stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Crawler stopped by user.")
