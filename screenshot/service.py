import asyncio
import logging
import binascii
import os
import io
import av
import libtorrent as lt
import threading
from collections import namedtuple, defaultdict, OrderedDict
from concurrent.futures import Future

KeyframeInfo = namedtuple('KeyframeInfo', ['pts', 'pos', 'size'])

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

        # LRU cache for pieces to reduce requests to the main thread
        self.piece_cache = OrderedDict()
        self.PIECE_CACHE_SIZE = 4  # Cache up to 4 pieces

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

            # The higher-level logic is responsible for ensuring the piece
            # has been prioritized for download.
            piece_data = None
            # Check cache first
            if piece_index in self.piece_cache:
                piece_data = self.piece_cache[piece_index]
                self.piece_cache.move_to_end(piece_index) # Mark as recently used
            else:
                # Cache miss: request the piece from the main thread
                future = Future()
                call_read_piece = False
                with self.service.read_lock:
                    # If we are the first to request this piece, we trigger the read.
                    if not self.service.pending_reads[piece_index]:
                        call_read_piece = True
                    self.service.pending_reads[piece_index].append(future)

                if call_read_piece:
                    self.handle.read_piece(piece_index)

                piece_data = future.result(timeout=60)

                # Update cache
                self.piece_cache[piece_index] = piece_data
                if len(self.piece_cache) > self.PIECE_CACHE_SIZE:
                    self.piece_cache.popitem(last=False) # Evict least recently used

            if piece_data is None:
                # This can happen if the read times out or fails.
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
            'dht_bootstrap_nodes': 'router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881',
            # Set a larger cache size (64 MB) to prevent piece eviction.
            # Even if docs say it's deprecated, it might be necessary for this binding.
            'cache_size': 4096,
            'suggest_mode': lt.suggest_mode_t.suggest_read_cache,
        }
        self.ses = lt.session(settings)

        self.task_queue = asyncio.Queue()
        self.workers = []
        self.alert_task = None
        self._running = False

        # Event to signal when DHT is bootstrapped and ready
        self.dht_ready = asyncio.Event()

        # Data structures to hold futures for pending events
        self.pending_metadata = {}
        # A dictionary mapping a piece index to a list of "waiter" objects.
        # Each waiter tracks a future and how many pieces that future is waiting for.
        self.piece_to_waiters = defaultdict(list)
        self.pending_reads = defaultdict(list)
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

    async def submit_task(self, infohash):
        await self.task_queue.put(infohash)
        self.log.info(f"Submitted task for infohash: {infohash}")

    def _handle_metadata_received(self, alert):
        infohash_str = str(alert.handle.info_hash())
        if infohash_str in self.pending_metadata:
            future = self.pending_metadata[infohash_str]
            if not future.done():
                future.set_result(alert.handle)

    def _handle_piece_finished(self, alert):
        # This is an O(1) lookup for the piece index.
        # We pop the list of waiters, as we won't get another alert for this piece.
        waiters_for_piece = self.piece_to_waiters.pop(alert.piece_index, [])

        for waiter in waiters_for_piece:
            waiter['remaining'] -= 1
            if waiter['remaining'] <= 0 and not waiter['future'].done():
                # This future is now resolved, its on_done callback will handle
                # removing the waiter from any other piece lists it might be in.
                waiter['future'].set_result(True)

    def _handle_torrent_finished(self, alert):
        # In case the torrent finishes while we are waiting for specific pieces,
        # resolve all pending futures. This is a broad signal, and a more
        # precise implementation would associate waiters with torrent handles.
        # This implementation preserves the original's behavior.

        # 1. Collect all unique, pending futures.
        futures_to_resolve = set()
        for waiters_list in self.piece_to_waiters.values():
            for waiter in waiters_list:
                if not waiter['future'].done():
                    futures_to_resolve.add(waiter['future'])

        # 2. Resolve all collected futures. The on_done callback on each future
        # will attempt to clean up its entries from the piece_to_waiters dict.
        for fut in futures_to_resolve:
            fut.set_result(True)

        # 3. As a final measure, clear the dictionary to prevent any stale entries.
        # This is safe because all legitimate waiters have been resolved.
        self.piece_to_waiters.clear()

    def _handle_dht_bootstrap(self, alert):
        if not self.dht_ready.is_set():
            self.dht_ready.set()
            self.log.debug("DHT bootstrapped. Service is ready to process tasks.")

    def _handle_dht_get_peers_reply(self, alert):
        self.log.debug(f"Received DHT peers reply for {alert.info_hash}. Found {len(alert.peers)} peers.")

    def _handle_tracker_reply(self, alert):
        self.log.debug(f"Received tracker reply for {alert.handle.info_hash()}. Found {alert.num_peers} peers.")

    def _handle_read_piece(self, alert):
        with self.read_lock:
            # There might be multiple futures waiting for the same piece
            futures = self.pending_reads.pop(alert.piece, [])

        # The `alert.error` is unreliable. We assume the buffer is valid.
        data = bytes(alert.buffer)
        for future in futures:
            if not future.done():
                future.set_result(data)

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
                await asyncio.sleep(0)
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
        waiter = {
            "future": future,
            "pieces": pieces_to_actually_wait_for,
            "remaining": len(pieces_to_actually_wait_for),
        }

        # Cleanup function to be called when the future is done (success or timeout)
        def on_done(fut):
            # Remove this waiter from all piece lists it was added to.
            # This is crucial to prevent memory leaks if the wait times out.
            for piece_index in waiter['pieces']:
                waiters_list = self.piece_to_waiters.get(piece_index)
                if waiters_list:
                    try:
                        # Use a loop for safe removal, as the same waiter object
                        # might have been added multiple times if logic were to change.
                        while waiter in waiters_list:
                            waiters_list.remove(waiter)
                    except ValueError:
                        pass # Should not happen with `in` check, but good practice

        future.add_done_callback(on_done)

        # Register the waiter for each piece it's waiting for
        for piece_index in pieces_to_actually_wait_for:
            self.piece_to_waiters[piece_index].append(waiter)

        try:
            await asyncio.wait_for(future, timeout=180)
        except asyncio.TimeoutError:
            self.log.warning(f"Timeout waiting for {len(waiter['pieces'])} pieces for {infohash_hex}.")
            # The on_done callback will have already been triggered and cleaned up.
            raise # Re-raise the timeout to be handled by the caller

    async def _handle_screenshot_task(self, infohash_hex):
        self.log.info(f"Processing task for {infohash_hex}")
        handle = None
        infohash_bytes = None
        # Use shared memory for saving torrent data to avoid disk I/O
        save_dir = f"/dev/shm/{infohash_hex}"
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
            self.log.debug(f"Successfully received metadata for {infohash_hex}")

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

            self.log.debug(f"Identified target video file: {target_file.path} (size: {target_file.size})")

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

            self.log.debug(f"Downloading header for {target_file.path} ({len(head_pieces)} pieces)...")
            self.log.debug("Waiting for header pieces...")
            await self._wait_for_pieces(infohash_hex, handle, head_pieces.copy())
            self.log.debug("Header pieces finished.")

            # Also download the footer, as the video index (moov atom) might be there.
            footer_size_to_download = 5 * 1024 * 1024
            footer_start_offset = max(0, target_file.size - footer_size_to_download)

            if footer_start_offset > header_size_to_download: # Avoid re-downloading if files are small
                start_piece_req_foot = ti.map_file(video_file_index, footer_start_offset, 1)
                end_piece_req_foot = ti.map_file(video_file_index, target_file.size - 1, 1)
                foot_pieces = set(range(start_piece_req_foot.piece, end_piece_req_foot.piece + 1))

                if foot_pieces:
                    for p in foot_pieces:
                        handle.piece_priority(p, 7)
                    self.log.debug(f"Downloading footer for {target_file.path} ({len(foot_pieces)} pieces)...")
                    await self._wait_for_pieces(infohash_hex, handle, foot_pieces.copy())
                    self.log.debug("Footer pieces finished.")

            self.log.debug(f"Header and footer for {target_file.path} downloaded.")

            # 4. Get video duration
            duration_sec = await self.loop.run_in_executor(
                None, self._get_video_duration, handle, video_file_index
            )
            if not duration_sec or duration_sec <= 0:
                self.log.error(f"Could not get a valid video duration for {infohash_hex}. Stopping task.")
                return

            # 5. Concurrently find all keyframes
            num_screenshots = 20
            timestamp_secs = [(duration_sec / (num_screenshots + 1)) * (i + 1) for i in range(num_screenshots)]

            find_tasks = [
                self.loop.run_in_executor(None, self._find_keyframe_for_timestamp, handle, video_file_index, ts)
                for ts in timestamp_secs
            ]
            packet_infos = await asyncio.gather(*find_tasks)

            # Filter out any failed lookups
            valid_packet_infos = [info for info in packet_infos if info and info[0]]

            if not valid_packet_infos:
                self.log.error(f"Could not find any valid keyframes for {infohash_hex}.")
                return

            # 6. Prioritize all necessary pieces at once
            all_pieces_needed = set()
            pieces_per_keyframe = []
            for keyframe_info, timestamp_str in valid_packet_infos:
                pieces = self._get_pieces_for_packet(ti, video_file_index, keyframe_info)
                pieces_per_keyframe.append(pieces)
                all_pieces_needed.update(pieces)

            self.log.info(f"Requesting {len(all_pieces_needed)} unique pieces for {len(valid_packet_infos)} screenshots.")
            for piece_index in all_pieces_needed:
                handle.piece_priority(piece_index, 7)

            # 7. Concurrently wait for pieces and decode
            decode_tasks = [
                self._wait_and_decode_one_screenshot(handle, video_file_index, infohash_hex, packet_info, pieces)
                for packet_info, pieces in zip(valid_packet_infos, pieces_per_keyframe)
            ]
            await asyncio.gather(*decode_tasks)

            self.log.info(f"Finished taking {len(valid_packet_infos)} screenshots for {infohash_hex}.")

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

    async def _wait_and_decode_one_screenshot(self, handle, video_file_index, infohash_hex, packet_info, pieces_needed):
        keyframe_info, timestamp_str = packet_info

        self.log.debug(f"Waiting for {len(pieces_needed)} pieces for timestamp {timestamp_str}")
        await self._wait_for_pieces(infohash_hex, handle, pieces_needed)
        self.log.debug(f"Pieces for timestamp {timestamp_str} are ready.")

        await self.loop.run_in_executor(
            None, self._decode_and_save_frame, handle, video_file_index, keyframe_info, infohash_hex, timestamp_str
        )

    def _get_video_duration(self, handle, video_file_index):
        """
        Reads the video metadata to determine its duration.
        NOTE: This is a blocking function and should be run in an executor.
        """
        self.log.debug("Opening video container to get duration...")
        torrent_reader = TorrentFileReader(self, handle, video_file_index)
        try:
            with av.open(torrent_reader) as container:
                # duration is in microseconds, convert to seconds
                duration_sec = container.duration / 1000000.0
                self.log.debug(f"Detected video duration: {duration_sec:.2f} seconds.")
                return duration_sec
        except Exception as e:
            self.log.error(f"Could not determine video duration: {e!r}")
            return 0

    def _find_keyframe_for_timestamp(self, handle, video_file_index, timestamp_sec):
        """
        Finds the keyframe packet closest to a given timestamp.
        NOTE: This is a blocking function.
        """
        m, s = divmod(timestamp_sec, 60)
        h, m = divmod(m, 60)
        timestamp_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

        torrent_reader = TorrentFileReader(self, handle, video_file_index)
        try:
            with av.open(torrent_reader) as container:
                stream = container.streams.video[0]
                # Seek to the frame before the target time
                container.seek(int(timestamp_sec * 1000000), backward=True, any_frame=False, stream=stream)

                # Find the next keyframe
                packet = None
                for p in container.demux(stream):
                    if p.dts is not None and p.is_keyframe:
                        packet = p
                        break

                if packet:
                    info = KeyframeInfo(pts=packet.pts, pos=packet.pos, size=packet.size)
                    self.log.debug(f"Found keyframe at {float(info.pts * stream.time_base):.2f}s for target {timestamp_str}")
                    return info, timestamp_str
                else:
                    self.log.warning(f"Could not find keyframe near {timestamp_str}")
                    return None, None

        except Exception as e:
            self.log.error(f"Error finding keyframe for {timestamp_str}: {e!r}")
            return None, None

    def _get_pieces_for_packet(self, ti, video_file_index, keyframe_info):
        """Calculates the set of pieces required for a given KeyframeInfo."""
        start_req = ti.map_file(video_file_index, keyframe_info.pos, 1)
        # The keyframe_info.size can be 0 sometimes, so read at least 1 byte
        read_size = max(1, keyframe_info.size)
        end_req = ti.map_file(video_file_index, keyframe_info.pos + read_size - 1, 1)
        return set(range(start_req.piece, end_req.piece + 1))

    def _decode_and_save_frame(self, handle, video_file_index, keyframe_info, infohash_hex, timestamp_str):
        """
        Decodes a single frame from a keyframe_info and saves it.
        Assumes the necessary pieces are already downloaded.
        NOTE: This is a blocking function.
        """
        self.log.debug(f"Decoding frame for timestamp {timestamp_str}")
        torrent_reader = TorrentFileReader(self, handle, video_file_index)
        try:
            with av.open(torrent_reader) as container:
                stream = container.streams.video[0]
                container.seek(keyframe_info.pts, stream=stream)

                for frame in container.decode(stream):
                    if frame.pts >= keyframe_info.pts:
                        # Use shared memory for screenshots to avoid disk I/O
                        output_dir = "/dev/shm/screenshots"
                        output_filename = f"{output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"
                        os.makedirs(output_dir, exist_ok=True)
                        frame.to_image().save(output_filename)
                        self.log.info(f"SUCCESS: Screenshot saved to {output_filename}")
                        return

                self.log.error(f"Failed to decode frame after re-seeking for timestamp {timestamp_str}")

        except Exception as e:
            self.log.error(f"Error decoding frame for {timestamp_str}: {e!r}")

    async def _worker(self):
        while self._running:
            got_task = False
            try:
                infohash = await self.task_queue.get()
                got_task = True
                await self._handle_screenshot_task(infohash)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("Error in screenshot worker.")
            finally:
                if got_task:
                    self.task_queue.task_done()
