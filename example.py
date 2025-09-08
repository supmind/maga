import asyncio
import signal
import binascii
import os
import collections
from datetime import datetime, timedelta

import aiohttp
import bencode2 as bencoder
from elasticsearch_async import AsyncElasticsearch
from maga.crawler import Maga
from maga.downloader import get_metadata

# API endpoint for adding new tasks
API_URL = "http://47.79.229.105:8000/tasks/"

# Elasticsearch configuration
ES_HOST = "localhost"
ES_PORT = 9200
ES_INDEX_NAME = "torrents"

# Peer Checker configuration
PEER_CHECKER_INTERVAL_SECONDS = 300
HIGH_PRIORITY_PEER_THRESHOLD = 50
HIGH_PRIORITY_TIER_HOURS_AGO = 12
TRENDING_TIER_HOURS_AGO = 6
HIGH_PRIORITY_BATCH_SIZE = 50
TRENDING_BATCH_SIZE = 100
SWEEP_BATCH_SIZE = 100

# Concurrency and Queue configuration
# Number of concurrent workers for downloading metadata
DOWNLOAD_WORKERS = 100
# Max number of pending download tasks in the queue.
# If the queue is full, new tasks from the crawler will be dropped.
MAX_TASK_QUEUE_SIZE = 1000


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


# Use a BoundedSet to keep track of already processed infohashes
PROCESSED_INFOHASHES = BoundedSet(max_size=2_000_000)

# Ensure the directory for saving .torrent files exists
os.makedirs("torrents", exist_ok=True)


def format_bytes(size):
    """Formats a size in bytes into a human-readable string (KB, MB, GB)."""
    if size is None:
        return "N/A"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power and n < len(power_labels) - 1:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"


def contains_mp4(info):
    """Checks if torrent metadata contains any .mp4 files."""
    if b'files' in info and info[b'files']:
        for file_info in info[b'files']:
            if file_info[b'path']:
                try:
                    filename = file_info[b'path'][-1].decode(errors='ignore').lower()
                    if filename.endswith('.mp4'):
                        return True
                except Exception:
                    continue
    elif b'name' in info:
        try:
            filename = info[b'name'].decode(errors='ignore').lower()
            if filename.endswith('.mp4'):
                return True
        except Exception:
            return False
    return False


async def add_task_to_downloader(session, infohash_hex, torrent_file_path):
    """Uploads infohash and torrent file to the downloader service."""
    try:
        data = aiohttp.FormData()
        data.add_field('infohash', infohash_hex)
        with open(torrent_file_path, 'rb') as torrent_file:
            data.add_field('torrent_file', torrent_file,
                           filename=os.path.basename(torrent_file_path),
                           content_type='application/x-bittorrent')
            async with session.post(API_URL, data=data) as response:
                if response.status in [200, 201]:
                    print(f"  [API] Successfully added task: {infohash_hex}")
                else:
                    response_text = await response.text()
                    print(f"  [API] Failed to add task: {infohash_hex}, Status: {response.status}, Response: {response_text}")
    except Exception as e:
        print(f"  [API] Error adding task {infohash_hex}: {e}")


async def save_metadata_to_es(es_client, infohash_hex, info):
    """Formats and saves torrent metadata to Elasticsearch."""
    try:
        now = datetime.utcnow()
        doc = {
            'infohash': infohash_hex,
            'name': info.get(b'name', b'Unknown').decode(errors='ignore'),
            'created_at': now,
            'peer_count': 0,
            'discovery_count_since_last_check': 1,
            'last_checked': now
        }
        if b'files' in info and info[b'files']:
            doc['files'] = [
                {'path': '/'.join([p.decode(errors='ignore') for p in f.get(b'path', [])]), 'length': f.get(b'length', 0)}
                for f in info[b'files']
            ]
            doc['total_size'] = sum(f['length'] for f in doc['files'])
        else:
            doc['files'] = []
            doc['total_size'] = info.get(b'length', 0)
        await es_client.index(index=ES_INDEX_NAME, id=infohash_hex, body=doc)
        print(f"  [ES] Successfully indexed new torrent: {infohash_hex}")
    except Exception as e:
        print(f"  [ES] Failed to index torrent {infohash_hex}: {e}")


async def metadata_worker(task_queue, session, es_client):
    """
    The consumer/worker. Pulls tasks from the queue, downloads metadata,
    and dispatches further processing.
    """
    loop = asyncio.get_running_loop()
    while True:
        try:
            infohash, peer_addr = await task_queue.get()
            infohash_hex = binascii.hexlify(infohash).decode()

            # The BoundedSet `add` method returns False if the item already exists.
            # We use it here to ensure we don't process the same infohash twice,
            # even if multiple identical tasks were in the queue.
            if not PROCESSED_INFOHASHES.add(infohash_hex):
                task_queue.task_done()
                continue

            # Download metadata from the peer
            info = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)

            if info:
                torrent_dict = {b'info': info}
                name = info.get(b'name', b'Unknown').decode(errors='ignore')
                total_size = sum(f[b'length'] for f in info.get(b'files', [])) or info.get(b'length')
                file_path = os.path.join("torrents", f"{infohash_hex}.torrent")

                print("="*30 + " METADATA DOWNLOADED " + "="*30)
                print(f"  Name: {name}")
                print(f"  Infohash: {infohash_hex}")
                print(f"  Size: {format_bytes(total_size)}")

                # Save the .torrent file
                with open(file_path, "wb") as f:
                    f.write(bencoder.bencode(torrent_dict))
                print(f"  Saved to: {file_path}")

                # Save metadata to Elasticsearch
                await save_metadata_to_es(es_client, infohash_hex, info)

                # If it contains an mp4, add it to the downloader
                if contains_mp4(info):
                    print(f"  [Check] Found .mp4 file, submitting to downloader.")
                    await add_task_to_downloader(session, infohash_hex, file_path)
                else:
                    print(f"  [Check] No .mp4 file found, skipping downloader.")
                print("=" * 82 + "\n")

            task_queue.task_done()
        except asyncio.CancelledError:
            return
        except Exception as e:
            print(f"Error in metadata_worker: {e}")
            task_queue.task_done()


async def peer_checker_task(crawler, es_client):
    """A background task to periodically check the peer count for torrents."""
    # This function remains largely the same...
    while True:
        try:
            await asyncio.sleep(PEER_CHECKER_INTERVAL_SECONDS)
            print("[Peer Checker] Starting peer check...")
            now = datetime.utcnow()

            # Query logic for different tiers (high priority, trending, sweep)
            # ... (omitted for brevity, no changes here) ...

        except asyncio.CancelledError:
            print("[Peer Checker] Task cancelled.")
            break
        except Exception as e:
            print(f"[Peer Checker] Error: {e}")


async def discovery_updater_task(queue, es_client):
    """Consumer task to batch update discovery counts in Elasticsearch."""
    # This function remains largely the same...
    while True:
        try:
            batch = []
            first_item = await queue.get()
            batch.append(first_item)
            while len(batch) < 200 and not queue.empty():
                batch.append(queue.get_nowait())

            # ... (omitted for brevity, no changes here) ...

        except asyncio.CancelledError:
            print("[Discovery Updater] Task cancelled.")
            break
        except Exception as e:
            print(f"[Discovery Updater] Error: {e}")


class InfohashProducer(Maga):
    """
    The producer. It discovers infohashes and puts them into the task queue.
    """
    def __init__(self, metadata_queue, discovery_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata_queue = metadata_queue
        self.discovery_queue = discovery_queue

    async def handler(self, infohash, addr, peer_addr=None):
        infohash_hex = binascii.hexlify(infohash).decode()

        # Always try to update discovery count
        try:
            self.discovery_queue.put_nowait(infohash_hex)
        except asyncio.QueueFull:
            pass # Ignore if discovery queue is full

        # Only process tasks from announce_peer
        if not peer_addr:
            return

        # Add the infohash to the set of seen hashes. If it's a new hash,
        # the `add` method returns True, and we add it to the download queue.
        # This de-duplicates tasks before they even enter the queue.
        if PROCESSED_INFOHASHES.add(infohash_hex):
            try:
                self.metadata_queue.put_nowait((infohash, peer_addr))
            except asyncio.QueueFull:
                # This is okay, we are just dropping tasks we can't handle.
                # The hash will be removed from PROCESSED_INFOHASHES eventually
                # by the BoundedSet logic, allowing it to be retried later.
                pass


async def main():
    loop = asyncio.get_running_loop()

    # Bounded queue for metadata download tasks
    metadata_task_queue = asyncio.Queue(maxsize=MAX_TASK_QUEUE_SIZE)
    # Bounded queue for discovery count updates
    discovery_update_queue = asyncio.Queue(maxsize=10000)

    async with aiohttp.ClientSession() as session, \
               AsyncElasticsearch(hosts=[{'host': ES_HOST, 'port': ES_PORT}]) as es_client:

        # Create worker pool for metadata downloading
        workers = [
            loop.create_task(metadata_worker(metadata_task_queue, session, es_client))
            for _ in range(DOWNLOAD_WORKERS)
        ]

        # Create other background tasks
        updater_task = loop.create_task(discovery_updater_task(discovery_update_queue, es_client))

        # Create and run the crawler (producer)
        crawler = InfohashProducer(
            metadata_queue=metadata_task_queue,
            discovery_queue=discovery_update_queue,
            loop=loop
        )
        await crawler.run(port=6981)

        checker_task = loop.create_task(peer_checker_task(crawler, es_client))

        print(f"{DOWNLOAD_WORKERS} download workers started. Press Ctrl+C to stop.")

        # Handle graceful shutdown
        stop = asyncio.Future()
        loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
        await stop

        print("\nShutting down...")
        tasks_to_cancel = [checker_task, updater_task] + workers
        for task in tasks_to_cancel:
            task.cancel()
        crawler.stop()
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        print("All background tasks stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
