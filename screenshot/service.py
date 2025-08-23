import asyncio
import logging
import binascii
import os
import av
import libtorrent as lt

class ScreenshotService:
    def __init__(self, loop=None, num_workers=10):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.log = logging.getLogger("ScreenshotService")

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

        # Event to signal when DHT is bootstrapped and ready
        self.dht_ready = asyncio.Event()

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
                pieces_to_wait_for.discard(alert.piece_index)
                if not pieces_to_wait_for:
                    future.set_result(True)

    def _handle_dht_bootstrap(self, alert):
        if not self.dht_ready.is_set():
            self.dht_ready.set()
            self.log.info("DHT bootstrapped. Service is ready to process tasks.")

    def _handle_dht_get_peers_reply(self, alert):
        self.log.info(f"Received DHT peers reply for {alert.info_hash}. Found {len(alert.peers)} peers.")

    def _handle_tracker_reply(self, alert):
        self.log.info(f"Received tracker reply for {alert.handle.info_hash()}. Found {alert.num_peers} peers.")

    async def _alert_loop(self):
        while self._running:
            try:
                if self.ses.wait_for_alert(1000):
                    alerts = self.ses.pop_alerts()
                    for alert in alerts:
                        # Log all alerts at DEBUG level
                        self.log.debug(f"Libtorrent Alert: {alert}")

                        # Handle specific alerts needed for logic
                        if isinstance(alert, lt.metadata_received_alert):
                            self._handle_metadata_received(alert)
                        elif isinstance(alert, lt.piece_finished_alert):
                            self._handle_piece_finished(alert)
                        elif isinstance(alert, lt.dht_bootstrap_alert):
                            self._handle_dht_bootstrap(alert)
                        elif isinstance(alert, lt.dht_get_peers_reply_alert):
                            self._handle_dht_get_peers_reply(alert)
                        elif isinstance(alert, lt.tracker_reply_alert):
                            self._handle_tracker_reply(alert)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("Error in libtorrent alert loop.")

    async def _wait_for_pieces(self, infohash_bytes, handle, pieces_to_download):
        future = self.loop.create_future()
        self.pending_pieces[str(infohash_bytes)] = (future, pieces_to_download)
        await asyncio.wait_for(future, timeout=180)
        self.pending_pieces.pop(str(infohash_bytes), None)

    async def _handle_screenshot_task(self, infohash_hex, timestamp):
        self.log.info(f"Processing task for {infohash_hex} at {timestamp}")
        handle = None
        infohash_bytes = None
        save_dir = f"./downloads/{infohash_hex}"
        os.makedirs(save_dir, exist_ok=True)

        try:
            # Wait for the DHT to be ready before proceeding
            await self.dht_ready.wait()

            # 1. Add torrent using a magnet link and wait for metadata
            future = self.loop.create_future()
            infohash_bytes = binascii.unhexlify(infohash_hex)
            self.pending_metadata[str(infohash_bytes)] = future

            trackers = [
                "udp://tracker.openbittorrent.com:80",
                "udp://tracker.opentrackr.org:1337/announce",
                "udp://tracker.coppersurfer.tk:6969/announce"
            ]
            tracker_str = "&".join([f"tr={t}" for t in trackers])
            magnet_uri = f"magnet:?xt=urn:btih:{infohash_hex}&{tracker_str}"

            params = lt.parse_magnet_uri(magnet_uri)
            params.save_path = save_dir
            handle = self.ses.add_torrent(params)

            self.log.debug(f"Added torrent for {infohash_hex}, waiting for metadata...")
            await asyncio.wait_for(future, timeout=180) # Increased timeout
            self.log.info(f"Successfully received metadata for {infohash_hex}")

            # 2. Find target video file
            ti = handle.get_torrent_info()
            video_file_index = -1
            target_file = None
            max_size = -1
            for i, f in enumerate(ti.files()):
                if f.path.lower().endswith((".mp4", ".mkv", ".avi")) and f.size > max_size:
                    max_size = f.size
                    video_file_index = i
                    target_file = f

            if video_file_index == -1:
                self.log.warning(f"No video file found in torrent {infohash_hex}")
                return

            video_path = os.path.join(save_dir, target_file.path)
            self.log.info(f"Identified target video file: {video_path} (size: {target_file.size})")

            # 3. Prioritize and download file header
            piece_size = ti.piece_length()
            header_size_to_download = 4 * 1024 * 1024
            head_pieces = {p.piece for p in ti.map_file(video_file_index, 0, header_size_to_download).file_slices}

            handle.set_piece_deadline(list(head_pieces)[0], 1000)
            for p in head_pieces:
                handle.piece_priority(p, lt.download_priority.top_priority)

            self.log.info(f"Downloading header for {target_file.path} ({len(head_pieces)} pieces)...")
            await self._wait_for_pieces(infohash_bytes, handle, head_pieces)
            self.log.info(f"Header for {target_file.path} downloaded.")

            # 4. Use PyAV to find the target keyframe
            with av.open(video_path) as container:
                target_seconds = sum(int(x) * 60 ** i for i, x in enumerate(reversed(timestamp.split(':'))))
                stream = container.streams.video[0]

                target_pts = target_seconds / stream.time_base
                packet = None
                container.seek(int(target_pts), backward=True, any_frame=False, stream=stream)
                for p in container.demux(stream):
                    if p.is_keyframe:
                        packet = p
                        break

                if not packet:
                    raise RuntimeError("Could not find a keyframe near the target timestamp.")

                self.log.info(f"Found keyframe at {packet.pts * stream.time_base:.2f}s for target {timestamp}")

                # 5. Prioritize and download pieces for the keyframe
                keyframe_pieces_map = ti.map_file(video_file_index, packet.pos, packet.size)
                keyframe_pieces = {p.piece for p in keyframe_pieces_map.file_slices}

                for p in keyframe_pieces:
                    handle.piece_priority(p, lt.download_priority.top_priority)

                self.log.info(f"Downloading keyframe data ({len(keyframe_pieces)} pieces)...")
                await self._wait_for_pieces(infohash_bytes, handle, keyframe_pieces)
                self.log.info("Keyframe data downloaded.")

                # 6. Take screenshot
                container.seek(packet.pts, stream=stream)
                frame = next(container.decode(stream), None)
                if frame:
                    output_filename = f"./screenshots/{infohash_hex}_{timestamp.replace(':', '-')}.jpg"
                    os.makedirs("./screenshots", exist_ok=True)
                    frame.to_image().save(output_filename)
                    self.log.info(f"SUCCESS: Screenshot saved to {output_filename}")
                else:
                    raise RuntimeError("Failed to decode frame after downloading pieces.")

        except asyncio.TimeoutError:
            self.log.error(f"Timeout during screenshot task for {infohash_hex}")
        except Exception:
            self.log.exception(f"Error processing {infohash_hex}")
        finally:
            # 7. Clean up
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
