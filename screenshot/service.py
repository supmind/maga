import asyncio
import logging
import binascii
import os
import io
import av
import libtorrent as lt
import threading
from concurrent.futures import Future

class LibtorrentError(Exception):
    def __init__(self, error_code):
        self.error_code = error_code
        super().__init__(f"Libtorrent error: {error_code.message()}")

# ==============================================================================
# TorrentFileReader: A file-like object for reading from a torrent
# ==============================================================================
class TorrentFileReader(io.RawIOBase):
    """
    A file-like object that reads from a libtorrent torrent, but is designed
    to be called from a separate thread. It blocks on `read()` until the
    main asyncio thread receives the data and fulfills a future.
    """
    def __init__(self, service, handle, file_index):
        super().__init__()
        self.service = service
        self.handle = handle
        self.ti = handle.get_torrent_info()
        self.file_storage = self.ti.files()
        self.file_index = file_index

        self.file_entry = self.file_storage.at(self.file_index)
        self.file_size = self.file_entry.size
        self.pos = 0

        self.log = logging.getLogger("TorrentFileReader")

    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
        elif whence == io.SEEK_END:
            self.pos = self.file_size + offset
        return self.pos

    def tell(self):
        return self.pos

    def read(self, size=-1):
        if size == -1:
            size = self.file_size - self.pos
        size = min(size, self.file_size - self.pos)
        if size <= 0:
            return b''

        result_buffer = bytearray(size)
        buffer_offset = 0
        bytes_to_go = size
        current_file_pos = self.pos

        piece_size = self.ti.piece_length()

        while bytes_to_go > 0:
            req = self.ti.map_file(self.file_index, current_file_pos, 1)
            piece_index = req.piece
            piece_offset = req.start
            bytes_available_in_piece = piece_size - piece_offset
            read_len = min(bytes_to_go, bytes_available_in_piece)

            if not self.handle.have_piece(piece_index):
                result_buffer[buffer_offset : buffer_offset + read_len] = b'\x00' * read_len
            else:
                future = Future()
                with self.service.read_lock:
                    self.service.pending_reads[piece_index] = future

                # This is now thread-safe because it's running in the executor.
                # The main event loop is free to process the read_piece_alert.
                self.handle.read_piece(piece_index)

                # This call will block the *current thread* (not the event loop)
                # until the future is resolved in the main thread's alert loop.
                piece_data = future.result(timeout=60)

                if piece_data is None:
                    raise IOError(f"Failed to read piece {piece_index}: read_piece returned None")

                chunk = piece_data[piece_offset : piece_offset + read_len]
                result_buffer[buffer_offset : buffer_offset + len(chunk)] = chunk

            buffer_offset += read_len
            bytes_to_go -= read_len
            current_file_pos += read_len

        self.pos += size
        return bytes(result_buffer)

# ==============================================================================
# ScreenshotService
# ==============================================================================
class ScreenshotService:
    def __init__(self, loop=None, num_workers=10):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.log = logging.getLogger("ScreenshotService")

        settings = {
            'listen_interfaces': '0.0.0.0:6881',
            'enable_dht': True,
            'alert_mask': lt.alert_category.error | lt.alert_category.status | lt.alert_category.dht | lt.alert_category.peer | lt.alert_category.tracker,
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
        self.pending_reads = {}
        self.read_lock = threading.Lock()

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

    def _handle_torrent_finished(self, alert):
        infohash_str = str(alert.handle.info_hash())
        if infohash_str in self.pending_pieces:
            future, _ = self.pending_pieces[infohash_str]
            if not future.done():
                self.log.debug(f"Torrent {infohash_str} finished, resolving pending piece future.")
                future.set_result(True)

    def _handle_dht_bootstrap(self, alert):
        if not self.dht_ready.is_set():
            self.dht_ready.set()
            self.log.info("DHT bootstrapped. Service is ready to process tasks.")

    def _handle_dht_get_peers_reply(self, alert):
        self.log.info(f"Received DHT peers reply for {alert.info_hash}. Found {len(alert.peers)} peers.")

    def _handle_tracker_reply(self, alert):
        self.log.info(f"Received tracker reply for {alert.handle.info_hash()}. Found {alert.num_peers} peers.")

    def _handle_read_piece(self, alert):
        with self.read_lock:
            future = self.pending_reads.pop(alert.piece, None)
        if future:
            # The `alert.error` is unreliable. The error message is "Success"
            # yet the error flag is set. We will assume the alert means the
            # buffer is valid.
            future.set_result(bytes(alert.buffer))

    async def _alert_loop(self):
        while self._running:
            try:
                # Poll for alerts non-blockingly
                alerts = self.ses.pop_alerts()
                for alert in alerts:
                    # If the alert is an error, log it as such.
                    # Otherwise, log it as debug to avoid cluttering the logs.
                    if alert.category() & lt.alert_category.error:
                        self.log.error(f"Libtorrent Alert: {alert}")
                    else:
                        self.log.debug(f"Libtorrent Alert: {alert}")

                    # Handle specific alerts needed for logic
                    if isinstance(alert, lt.metadata_received_alert):
                        self._handle_metadata_received(alert)
                    elif isinstance(alert, lt.piece_finished_alert):
                        self._handle_piece_finished(alert)
                    elif isinstance(alert, lt.torrent_finished_alert):
                        self._handle_torrent_finished(alert)
                    elif isinstance(alert, lt.read_piece_alert):
                        self._handle_read_piece(alert)
                    elif isinstance(alert, lt.dht_bootstrap_alert):
                        self._handle_dht_bootstrap(alert)
                    elif isinstance(alert, lt.dht_get_peers_reply_alert):
                        self._handle_dht_get_peers_reply(alert)
                    elif isinstance(alert, lt.tracker_reply_alert):
                        self._handle_tracker_reply(alert)

                # Sleep to yield control to the event loop
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("Error in libtorrent alert loop.")

    async def _wait_for_pieces(self, infohash_hex, handle, pieces_to_download):
        # Filter out pieces that are already downloaded to avoid waiting for nothing.
        pieces_to_actually_wait_for = {p for p in pieces_to_download if not handle.have_piece(p)}

        if not pieces_to_actually_wait_for:
            self.log.debug(f"All {len(pieces_to_download)} required pieces are already present.")
            return

        self.log.debug(f"Waiting for {len(pieces_to_actually_wait_for)} out of {len(pieces_to_download)} requested pieces.")

        future = self.loop.create_future()
        self.pending_pieces[infohash_hex] = (future, pieces_to_actually_wait_for)
        try:
            await asyncio.wait_for(future, timeout=180)
        finally:
            # Ensure we clean up the pending future key, even if we time out
            self.pending_pieces.pop(infohash_hex, None)

    async def _handle_screenshot_task(self, infohash_hex, timestamp):
        self.log.info(f"Processing task for {infohash_hex} at {timestamp}")
        handle = None
        infohash_bytes = None
        save_dir = f"/tmp/downloads/{infohash_hex}"
        os.makedirs(save_dir, exist_ok=True)

        try:
            infohash_bytes = binascii.unhexlify(infohash_hex)

            # 1. Add torrent using a magnet link.
            # This will trigger the DHT to start if it hasn't already.
            future = self.loop.create_future()
            self.pending_metadata[infohash_hex] = future

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
            self.log.debug(f"Added torrent for {infohash_hex}.")

            # Wait for the DHT to be ready before proceeding
            self.log.debug("Waiting for DHT to be ready...")
            await self.dht_ready.wait()
            self.log.debug("DHT is ready.")

            # Now wait for the metadata to be received
            self.log.debug(f"Waiting for metadata...")
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

            self.log.info(f"Identified target video file: {target_file.path} (size: {target_file.size})")

            # 3. Prioritize and download file header
            piece_size = ti.piece_length()
            # Let's download a bit more for the header to be safe with various video formats
            header_size_to_download = 5 * 1024 * 1024
            # Ensure we don't try to download more than the file size
            header_size_to_download = min(header_size_to_download, target_file.size)

            start_piece_req = ti.map_file(video_file_index, 0, 1)
            # To get the end piece, we map the last byte of the range.
            last_byte_offset = max(0, header_size_to_download - 1)
            end_piece_req = ti.map_file(video_file_index, last_byte_offset, 1)
            head_pieces = set(range(start_piece_req.piece, end_piece_req.piece + 1))

            if not head_pieces:
                self.log.warning(f"No header pieces to download for {target_file.path}. The file might be empty.")
                return

            for p in head_pieces:
                handle.piece_priority(p, 7)

            self.log.info(f"Downloading header for {target_file.path} ({len(head_pieces)} pieces)...")
            self.log.debug("Waiting for header pieces...")
            await self._wait_for_pieces(infohash_hex, handle, head_pieces.copy())
            self.log.debug("Header pieces finished.")
            self.log.info(f"Header for {target_file.path} downloaded.")

            # 4. Run the blocking PyAV operations in a separate thread
            await self.loop.run_in_executor(
                None, self._process_video_file, handle, video_file_index, timestamp, infohash_hex
            )

        except asyncio.TimeoutError:
            self.log.error(f"Timeout during screenshot task for {infohash_hex}")
        except Exception:
            self.log.exception(f"Error processing {infohash_hex}")
        finally:
            # 7. Clean up
            if infohash_hex:
                self.pending_metadata.pop(infohash_hex, None)
            if handle:
                self.ses.remove_torrent(handle, lt.session.delete_files)

    def _process_video_file(self, handle, video_file_index, timestamp, infohash_hex):
        ti = handle.get_torrent_info()

        # 4. Use PyAV with our custom file-like object to find the target keyframe
        self.log.debug("Opening video container to find keyframe...")
        torrent_reader = TorrentFileReader(self, handle, video_file_index)

        with av.open(torrent_reader) as container:
            target_seconds = sum(int(x) * 60 ** i for i, x in enumerate(reversed(timestamp.split(':'))))
            stream = container.streams.video[0]

            container.seek(int(target_seconds * 1000000), backward=True, any_frame=False, stream=stream)

            packet = None
            for p in container.demux(stream):
                if p.dts is None: continue
                if p.is_keyframe:
                    packet = p
                    break

            if not packet:
                raise RuntimeError("Could not find a keyframe near the target timestamp.")

            self.log.info(f"Found keyframe at {float(packet.pts * stream.time_base):.2f}s for target {timestamp}")

            # 5. Prioritize and download pieces for the keyframe
            keyframe_pos = packet.pos
            keyframe_size = packet.size
            start_piece_req = ti.map_file(video_file_index, keyframe_pos, 1)
            last_byte_offset = max(0, keyframe_pos + keyframe_size - 1)
            end_piece_req = ti.map_file(video_file_index, last_byte_offset, 1)
            keyframe_pieces = set(range(start_piece_req.piece, end_piece_req.piece + 1))

            for p in keyframe_pieces:
                # We need to call piece_priority from the main event loop thread
                self.loop.call_soon_threadsafe(handle.piece_priority, p, 7)

            # This is a blocking call, but it's okay since we are in a worker thread.
            # We create a new future on the main event loop to wait for the pieces.
            future = asyncio.run_coroutine_threadsafe(
                self._wait_for_pieces(infohash_hex, handle, keyframe_pieces.copy()),
                self.loop
            )
            future.result(timeout=180) # Block this thread waiting for the result
            self.log.info("Keyframe data downloaded.")

            # 6. Take screenshot by re-seeking and decoding in the same container
            self.log.debug("Re-seeking to keyframe to decode...")
            container.seek(packet.pts, stream=stream)

            # Decode frames until we get the one at or after our target packet's timestamp
            for frame in container.decode(video=0):
                if frame.pts >= packet.pts:
                    output_dir = "/tmp/screenshots"
                    output_filename = f"{output_dir}/{infohash_hex}_{timestamp.replace(':', '-')}.jpg"
                    os.makedirs(output_dir, exist_ok=True)
                    frame.to_image().save(output_filename)
                    self.log.info(f"SUCCESS: Screenshot saved to {output_filename}")
                    return # Exit the function successfully

            raise RuntimeError("Failed to decode frame after re-seeking.")

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
