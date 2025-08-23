import asyncio
import asyncio
import libtorrent as lt
import logging
import binascii
import os
import av

class ScreenshotService:
    def __init__(self, loop=None, num_workers=10):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.log = logging.getLogger("ScreenshotService")

        # Configure and start the libtorrent session
        settings = {
            'listen_interfaces': '0.0.0.0:6881',
            'enable_dht': True,
            'alert_mask': lt.alert_category.all,
            'dht_bootstrap_nodes': 'router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881'
        }
        self.ses = lt.session(settings)

        self.task_queue = asyncio.Queue()
        self.workers = []
        self.alert_task = None
        self._running = False

        # Dictionaries to hold futures for pending events
        self.pending_metadata = {}
        self.pending_pieces = {}

    async def run(self):
        self.log.info("Starting Screenshot Service...")
        self._running = True

        self.alert_task = self.loop.create_task(self._alert_loop())

        for _ in range(self.num_workers):
            worker_task = self.loop.create_task(self._worker())
            self.workers.append(worker_task)

        self.log.info(f"Screenshot Service started with {self.num_workers} workers.")

    def stop(self):
        self.log.info("Stopping Screenshot Service...")
        self._running = False

        if self.alert_task:
            self.alert_task.cancel()
        for worker in self.workers:
            worker.cancel()

        del self.ses
        self.log.info("Screenshot Service stopped.")

    async def submit_task(self, infohash, timestamp):
        await self.task_queue.put((infohash, timestamp))
        self.log.info(f"Submitted task for infohash: {infohash}")

    def _handle_metadata_received(self, alert):
        infohash_str = str(alert.handle.info_hash())
        if infohash_str in self.pending_metadata:
            future = self.pending_metadata[infohash_str]
            if not future.done():
                future.set_result(alert.handle)

    def _handle_piece_finished(self, alert):
        infohash_str = str(alert.handle.info_hash())
        if infohash_str in self.pending_pieces:
            future, pieces_to_wait_for = self.pending_pieces[infohash_str]

            if not future.done():
                # Remove the finished piece from our waiting set
                pieces_to_wait_for.discard(alert.piece_index)

                # If the set is empty, all requested pieces are done
                if not pieces_to_wait_for:
                    future.set_result(True)

    async def _alert_loop(self):
        while self._running:
            try:
                if self.ses.wait_for_alert(1000):
                    alerts = self.ses.pop_alerts()
                    for alert in alerts:
                        self.log.debug(f"Libtorrent Alert: {alert}")
                        if isinstance(alert, lt.metadata_received_alert):
                            self._handle_metadata_received(alert)
                        elif isinstance(alert, lt.piece_finished_alert):
                            self._handle_piece_finished(alert)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("Error in libtorrent alert loop.")

    async def _handle_screenshot_task(self, infohash_hex, timestamp):
        self.log.info(f"Processing task for {infohash_hex} at {timestamp}")
        handle = None
        infohash_bytes = None
        save_dir = "./downloads"
        os.makedirs(save_dir, exist_ok=True)

        try:
            infohash_bytes = binascii.unhexlify(infohash_hex)
        except binascii.Error:
            self.log.error(f"Invalid infohash format: {infohash_hex}")
            return

        try:
            # 1. Add torrent and wait for metadata
            future = self.loop.create_future()
            self.pending_metadata[str(infohash_bytes)] = future

            params = {'info_hash': infohash_bytes, 'save_path': save_dir}
            handle = self.ses.add_torrent(params)

            self.log.debug(f"Added torrent for {infohash_hex}, waiting for metadata...")
            await asyncio.wait_for(future, timeout=60)
            self.log.info(f"Successfully received metadata for {infohash_hex}")

            # 2. Find MP4 file and prioritize its header
            ti = handle.get_torrent_info()
            video_file_index = -1
            target_file = None
            for i, f in enumerate(ti.files()):
                if f.path.lower().endswith((".mp4", ".mkv", ".avi")):
                    video_file_index = i
                    target_file = f
                    break

            if video_file_index == -1:
                self.log.warning(f"No video file found in torrent {infohash_hex}")
                return

            video_path = os.path.join(save_dir, target_file.path)
            self.log.info(f"Found video file: {video_path}")

            # 4. Use PyAV to open the file and take a screenshot
            # libtorrent will download pieces on demand as pyav tries to read the file.

            # Set all files to priority 0, then target file to 1
            for i in range(ti.num_files()):
                handle.file_priority(i, lt.download_priority.dont_download)
            handle.file_priority(video_file_index, lt.download_priority.default_priority)

            # Give libtorrent a moment to apply priorities and connect to peers
            await asyncio.sleep(5)

            with av.open(video_path) as container:
                # Seek to the timestamp. PyAV will trigger reads, and libtorrent will download the necessary pieces.
                # This is a simplified approach; a more advanced one would calculate keyframes.
                # For web-optimized files, seeking near the start is usually fast.
                target_seconds = sum(int(x) * 60 ** i for i, x in enumerate(reversed(timestamp.split(':'))))
                container.seek(target_seconds * 1000000) # Seek expects microseconds

                # Grab the next frame
                frame = next(container.decode(video=0), None)
                if frame:
                    output_filename = f"./screenshots/{infohash_hex}_{timestamp.replace(':', '-')}.jpg"
                    os.makedirs("./screenshots", exist_ok=True)
                    frame.to_image().save(output_filename)
                    self.log.info(f"SUCCESS: Screenshot saved to {output_filename}")
                else:
                    self.log.error(f"Could not decode a frame at {timestamp} for {infohash_hex}")

        except asyncio.TimeoutError:
            self.log.error(f"Timeout during screenshot task for {infohash_hex}")
        except Exception:
            self.log.exception(f"Error processing {infohash_hex}")
        finally:
            # Clean up
            if infohash_bytes:
                self.pending_metadata.pop(str(infohash_bytes), None)
            if handle:
                self.ses.remove_torrent(handle, lt.session.delete_files)


    async def _worker(self):
        while self._running:
            try:
                infohash, timestamp = await self.task_queue.get()
                await self._handle_screenshot_task(infohash, timestamp)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("Error in screenshot worker.")
            finally:
                self.task_queue.task_done()
