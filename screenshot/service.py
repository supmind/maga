import asyncio
import logging
import binascii
import os
import io
import av
import libtorrent as lt

# ==============================================================================
# TorrentFileReader: A file-like object for reading from a torrent
# ==============================================================================
class TorrentFileReader(io.RawIOBase):
    """
    一个“稀疏”的、只读的、可seek的文件类对象，用于从libtorrent中读取数据。
    它模拟一个完整的文件，允许PyAV等库直接从torrent数据流中读取，而无需先将整个文件下载到磁盘。
    当读取尚未下载的数据片段时，它会返回零字节。
    """
    def __init__(self, handle, file_index):
        super().__init__()
        self.handle = handle
        self.ti = handle.get_torrent_info()
        self.file_storage = self.ti.files()
        self.file_index = file_index

        self.file_entry = self.file_storage.at(self.file_index)
        self.file_size = self.file_entry.size
        self.pos = 0

        self.log = logging.getLogger("TorrentFileReader")
        # self.log.setLevel(logging.DEBUG)

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

        self.log.debug(f"Seek to {self.pos} (whence={whence}, offset={offset})")
        return self.pos

    def tell(self):
        return self.pos

    def read(self, size=-1):
        if size == -1:
            size = self.file_size - self.pos

        size = min(size, self.file_size - self.pos)
        if size <= 0:
            return b''

        self.log.debug(f"Reading {size} bytes from position {self.pos}")

        result_buffer = bytearray(size)
        buffer_offset = 0
        bytes_to_go = size
        current_file_pos = self.pos

        piece_size = self.ti.piece_length()

        while bytes_to_go > 0:
            # Map the current file position to a piece request.
            # This gives us the piece index and the starting offset within that piece.
            req = self.ti.map_file(self.file_index, current_file_pos, 1)
            piece_index = req.piece
            piece_offset = req.start

            # Calculate how many bytes we can read from this specific piece,
            # starting from the given offset.
            bytes_available_in_piece = piece_size - piece_offset

            # We only need to read what's left for our buffer, or what's left in the piece.
            read_len = min(bytes_to_go, bytes_available_in_piece)

            if self.handle.have_piece(piece_index):
                # If we have the piece, read the relevant chunk from it.
                self.log.debug(f"Reading {read_len} bytes from piece {piece_index} at offset {piece_offset}")
                piece_data = self.handle.read_piece(piece_index)
                chunk = piece_data[piece_offset : piece_offset + read_len]
                result_buffer[buffer_offset : buffer_offset + len(chunk)] = chunk
            else:
                # If we don't have the piece, we return zero bytes as this is a sparse reader.
                self.log.debug(f"Returning {read_len} zero bytes for missing piece {piece_index}")
                # The bytearray is already zero-filled, so we just advance the pointers.

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

            # 4. Use PyAV with our custom file-like object to find the target keyframe
            self.log.debug("Opening video container to find keyframe...")
            # PyAV需要一个可seek的对象，但我们只下载了部分文件。
            # 因此我们创建一个“稀疏文件”读取器，它模拟一个完整的文件。
            # 读取已下载的部分时，它返回真实数据；读取未下载的部分时，它返回零。
            torrent_reader = TorrentFileReader(handle, video_file_index)

            with av.open(torrent_reader) as container:
                target_seconds = sum(int(x) * 60 ** i for i, x in enumerate(reversed(timestamp.split(':'))))
                stream = container.streams.video[0]

                # PyAV will seek and read from our TorrentFileReader to find the keyframe
                container.seek(int(target_seconds * 1000000), backward=True, any_frame=False, stream=stream)

                packet = None
                for p in container.demux(stream):
                    if p.dts is None:  # Skip packets without a timestamp
                        continue
                    if p.is_keyframe:
                        packet = p
                        break

                if not packet:
                    raise RuntimeError("Could not find a keyframe near the target timestamp.")

                self.log.info(f"Found keyframe at {packet.pts * stream.time_base:.2f}s for target {timestamp}")

                # 5. Prioritize and download pieces for the keyframe
                # The packet.pos gives the byte offset of the keyframe in the file
                keyframe_pos = packet.pos
                keyframe_size = packet.size

                start_piece_req = ti.map_file(video_file_index, keyframe_pos, 1)
                last_byte_offset = max(0, keyframe_pos + keyframe_size - 1)
                end_piece_req = ti.map_file(video_file_index, last_byte_offset, 1)
                keyframe_pieces = set(range(start_piece_req.piece, end_piece_req.piece + 1))

                for p in keyframe_pieces:
                    handle.piece_priority(p, 7)

                self.log.info(f"Downloading keyframe data ({len(keyframe_pieces)} pieces from position {keyframe_pos})...")
                self.log.debug("Waiting for keyframe pieces...")
                await self._wait_for_pieces(infohash_hex, handle, keyframe_pieces.copy())
                self.log.debug("Keyframe pieces finished.")
                self.log.info("Keyframe data downloaded.")

                # 6. Take screenshot
                self.log.debug("Opening final container for screenshot...")
                # A new reader instance is cleaner as the old one's state is uncertain.
                final_reader = TorrentFileReader(handle, video_file_index)
                with av.open(final_reader) as final_container:
                    final_stream = final_container.streams.video[0]
                    # Seek directly to the keyframe packet's byte position for precision.
                    final_container.seek(packet.pos, whence='byte')

                    frame = None
                    # Use a more robust, explicit demux/decode loop.
                    for p in final_container.demux(final_stream):
                        # Skip packets that are not from our target video stream
                        if p.dts is None or p.stream.index != final_stream.index:
                            continue

                        # Decode the packet. It may contain multiple frames.
                        frames = p.decode()
                        if frames:
                            frame = frames[0]
                            break  # Success, we got our frame.

                    if frame:
                        output_dir = "/tmp/screenshots"
                        output_filename = f"{output_dir}/{infohash_hex}_{timestamp.replace(':', '-')}.jpg"
                        os.makedirs(output_dir, exist_ok=True)
                        frame.to_image().save(output_filename)
                        self.log.info(f"SUCCESS: Screenshot saved to {output_filename}")
                    else:
                        raise RuntimeError("Failed to decode frame after downloading keyframe pieces.")

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
