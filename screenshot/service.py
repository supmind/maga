import asyncio
import bisect
import logging
import binascii
import os
import io
import struct
import av
import libtorrent as lt
import threading
from collections import namedtuple, defaultdict, OrderedDict
from concurrent.futures import Future

KeyframeInfo = namedtuple('KeyframeInfo', ['pts', 'pos', 'size'])


class LibtorrentError(Exception):
    """自定义异常，用于传递来自 libtorrent 的特定错误。"""
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
        self.PIECE_CACHE_SIZE = 16  # Cache up to 16 pieces

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
                # 关键修复：在请求一个piece之前，必须把它设为高优先级，否则优先级为0的piece可能不会被下载。
                # 关键修复：在请求一个piece之前，必须把它设为高优先级，否则优先级为0的piece可能不会被下载。
                self.handle.piece_priority(piece_index, 7)
                self.log.debug(f"TorrentFileReader: 请求 piece {piece_index} (高优先级)")
                future = Future()
                call_read_piece = False
                # 使用新的锁来确保线程安全
                with self.service.pending_reads_lock:
                    # 如果我们是第一个请求这个 piece 的，我们就触发 read_piece 调用
                    if not self.service.pending_reads[piece_index]:
                        call_read_piece = True
                    self.service.pending_reads[piece_index].append(future)

                if call_read_piece:
                    self.handle.read_piece(piece_index)

                try:
                    # 健壮性修复：捕获可能由 _handle_read_piece 设置的异常
                    piece_data = future.result(timeout=300)
                except LibtorrentError as e:
                    self.log.error(f"TorrentFileReader: 从主线程获取 piece {piece_index} 失败: {e}")
                    # 将 LibtorrentError 转换为 IOError，这是文件类接口的标准做法
                    raise IOError(f"读取 piece {piece_index} 失败，libtorrent 错误: {e.error_code.message()}") from e

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
    def __init__(self, loop=None, num_workers=10, output_dir='./screenshots_output'):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.output_dir = output_dir
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
        self.pending_reads_lock = threading.Lock()

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
        # 关键修复：使用与 TorrentFileReader 中相同的锁，以防止竞态条件
        with self.pending_reads_lock:
            # 可能会有多个 future 在等待同一个 piece
            futures = self.pending_reads.pop(alert.piece, [])

        # 健壮性修复：检查 alert 中是否存在错误
        if alert.error:
            self.log.error(f"读取 piece {alert.piece} 失败: {alert.error.message()}")
            # 将异常设置到 future 上，以便 TorrentFileReader 可以捕获它
            error = LibtorrentError(alert.error)
            for future in futures:
                if not future.done():
                    future.set_exception(error)
            return

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

    @staticmethod
    def _parse_mp4_boxes(reader):
        """
        一个健壮的MP4 box解析器生成器。
        它会读取box的大小和类型，然后跳过其内容，这对于快速扫描文件结构至关重要。
        NOTE: 这是一个阻塞函数，应该在执行器中运行。
        """
        # 兼容 TorrentFileReader 和 BytesIO
        try:
            file_size = reader.file_size
            reader.seek(0)
        except AttributeError:
            # BytesIO 对象没有 file_size，需要手动计算
            reader.seek(0, io.SEEK_END)
            file_size = reader.tell()
            reader.seek(0)

        while reader.tell() < file_size:
            current_pos = reader.tell()
            try:
                header = reader.read(8)
                if len(header) < 8:
                    break

                size, box_type_bytes = struct.unpack('>I4s', header)
                box_type = box_type_bytes.decode('ascii', errors='ignore')

                header_size = 8
                if size == 1: # 64-bit size
                    size_bytes = reader.read(8)
                    if len(size_bytes) < 8:
                        break
                    size = struct.unpack('>Q', size_bytes)[0]
                    header_size = 16
                elif size == 0: # To end of file
                    size = reader.file_size - current_pos

                if size < header_size:
                    break # Invalid size

                content_pos = current_pos + header_size
                content_size = size - header_size
                yield box_type, content_pos, content_size

                next_box_pos = current_pos + size
                if next_box_pos <= current_pos:
                    break
                reader.seek(next_box_pos)

            except (IOError, struct.error):
                break

    def _is_fmp4(self, torrent_reader):
        """
        通过检查 'moov' -> 'mvex' box 的存在来判断是否为 fMP4。
        NOTE: 这是一个阻塞函数，应该在执行器中运行。
        """
        self.log.debug("正在检查文件是否为 fMP4...")
        torrent_reader.seek(0)
        try:
            for box_type, content_pos, content_size in self._parse_mp4_boxes(torrent_reader):
                if box_type == 'moov':
                    self.log.debug(f"发现 'moov' box at {content_pos-8}, size {content_size+8}")
                    torrent_reader.seek(content_pos)
                    moov_content_reader = io.BytesIO(torrent_reader.read(content_size))
                    for sub_box_type, _, _ in self._parse_mp4_boxes(moov_content_reader):
                        if sub_box_type == 'mvex':
                            self.log.info("在 'moov' box 中发现 'mvex' box，确认为 fMP4。")
                            return True
                    break
        except Exception as e:
            self.log.error(f"检查 fMP4 时出错: {e!r}", exc_info=True)

        self.log.info("未发现 'mvex' box，文件被视为标准 MP4。")
        return False

    def _parse_sidx(self, reader, box_pos, box_size):
        """解析sidx（Segment Index Box）。"""
        reader.seek(box_pos)
        sidx_data = io.BytesIO(reader.read(box_size))

        version = int.from_bytes(sidx_data.read(1), 'big')
        sidx_data.read(3) # flags

        sidx_data.read(4) # reference_ID
        timescale = int.from_bytes(sidx_data.read(4), 'big')

        if version == 0:
            earliest_presentation_time = int.from_bytes(sidx_data.read(4), 'big')
            first_offset = int.from_bytes(sidx_data.read(4), 'big')
        else:
            earliest_presentation_time = int.from_bytes(sidx_data.read(8), 'big')
            first_offset = int.from_bytes(sidx_data.read(8), 'big')

        sidx_data.read(2) # reserved
        reference_count = int.from_bytes(sidx_data.read(2), 'big')

        references = []
        current_pts = earliest_presentation_time
        for _ in range(reference_count):
            ref_header = sidx_data.read(4)
            reference_size = int.from_bytes(ref_header, 'big') & 0x7FFFFFFF # 31 bits

            subsegment_duration = int.from_bytes(sidx_data.read(4), 'big')

            sap_header = sidx_data.read(4)
            starts_with_sap = (sap_header[0] & 0x80) >> 7

            references.append({
                'size': reference_size,
                'duration_pts': subsegment_duration,
                'pts': current_pts,
                'is_keyframe': bool(starts_with_sap)
            })
            current_pts += subsegment_duration

        return references, timescale, first_offset

    def _find_and_parse_keyframes_fmp4(self, torrent_reader):
        """fMP4关键帧解析的核心逻辑。这是一个阻塞函数。"""
        all_keyframes = []
        total_duration_pts = 0
        timescale = 0

        torrent_reader.seek(0)
        sidx_segments = []
        sidx_timescale = 0
        sidx_first_offset = 0
        sidx_pos = 0
        sidx_size = 0

        # 1. 查找并解析 sidx box
        for box_type, content_pos, content_size in self._parse_mp4_boxes(torrent_reader):
            if box_type == 'sidx':
                sidx_pos = content_pos - 8
                sidx_size = content_size + 8
                self.log.debug(f"发现 'sidx' box at {sidx_pos}, size {sidx_size}")
                sidx_segments, sidx_timescale, sidx_first_offset = self._parse_sidx(torrent_reader, content_pos, content_size)
                timescale = sidx_timescale
                break

        if not sidx_segments:
            self.log.warning("在文件中未找到 'sidx' box。无法进行fMP4优化提取。")
            return [], 0, 0

        # sidx 中的 first_offset 是相对于 sidx box 自身结束位置的偏移量
        data_start_offset = sidx_pos + sidx_size + sidx_first_offset

        # 2. 从 sidx 引用中提取关键帧信息
        current_offset = data_start_offset
        for seg in sidx_segments:
            if seg['is_keyframe']:
                all_keyframes.append(KeyframeInfo(pts=seg['pts'], pos=current_offset, size=seg['size']))
            total_duration_pts += seg['duration_pts']
            current_offset += seg['size']

        self.log.info(f"通过 sidx 解析发现 {len(all_keyframes)} 个潜在的关键帧段。")
        duration_sec = total_duration_pts / timescale if timescale else 0
        return all_keyframes, duration_sec, timescale

    async def _extract_keyframes_fmp4(self, handle, video_file_index):
        """
        从fMP4文件中提取关键帧信息。
        此方法通过按需读取和解析sidx box来定位关键帧，以实现最小化下载。
        """
        self.log.info("开始从fMP4文件中提取关键帧...")

        # TorrentFileReader会按需下载数据，我们不需要预先下载整个文件。
        # _find_and_parse_keyframes_fmp4 是一个阻塞函数，在执行器中运行。
        # 它会使用 _parse_mp4_boxes，后者在调用 read() 时触发 TorrentFileReader 的下载逻辑。
        torrent_reader = TorrentFileReader(self, handle, video_file_index)
        all_keyframes, duration_sec, timescale = await self.loop.run_in_executor(
            None, self._find_and_parse_keyframes_fmp4, torrent_reader
        )

        if not all_keyframes or duration_sec <= 0:
            self.log.error("在fMP4文件中未能提取到任何关键帧或有效时长。")
            return []

        if timescale == 0:
            timescale = 90000 # 如果无法从sidx中获取，使用一个常见的默认值
            self.log.warning(f"无法从sidx中精确获取timescale，使用默认值: {timescale}")

        # 从所有关键帧中均匀选取约20个
        keyframe_pts = [kf.pts for kf in all_keyframes]

        if duration_sec > 3600:
            num_screenshots = int(duration_sec / 180)
        else:
            num_screenshots = 20

        timestamp_secs = [(duration_sec / (num_screenshots + 1)) * (i + 1) for i in range(num_screenshots)]

        valid_packet_infos = []
        seen_pts = set()
        for ts in timestamp_secs:
            target_pts = int(ts * timescale)
            insertion_point = bisect.bisect_right(keyframe_pts, target_pts)
            if insertion_point > 0:
                best_keyframe_index = insertion_point - 1
                keyframe_info = all_keyframes[best_keyframe_index]

                if keyframe_info.pts not in seen_pts:
                    m, s = divmod(ts, 60)
                    h, m = divmod(m, 60)
                    timestamp_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"
                    valid_packet_infos.append((keyframe_info, timestamp_str))
                    seen_pts.add(keyframe_info.pts)

        self.log.info(f"为fMP4文件选择了 {len(valid_packet_infos)} 个截图点。")
        return valid_packet_infos


    async def _extract_keyframes_strategically(self, handle, video_file_index, infohash_hex):
        """
        使用多种策略智能地提取关键帧信息，以提高成功率并遵守最小化下载原则。
        此函数会按顺序尝试最高效的策略。
        """
        self.log.info("开始策略性关键帧提取...")
        ti = handle.get_torrent_info()
        target_file = ti.files().at(video_file_index)
        # 创建一个贯穿此函数始终的 TorrentFileReader 实例
        torrent_reader = TorrentFileReader(self, handle, video_file_index)

        # --- 策略一: 尝试作为 fMP4 使用 sidx box 解析 ---
        self.log.info("策略一: 尝试通过 fMP4/sidx 方法提取关键帧...")
        # 我们需要下载少量数据来检查文件类型和查找 sidx。
        # 假设 sidx (如果存在) 会在文件的前 64KB 内。
        header_size_to_download = 64 * 1024
        header_size_to_download = min(header_size_to_download, target_file.size)

        head_pieces = set()
        if header_size_to_download > 0:
            start_piece_req = ti.map_file(video_file_index, 0, 1)
            end_piece_req = ti.map_file(video_file_index, header_size_to_download - 1, 1)
            head_pieces = set(range(start_piece_req.piece, end_piece_req.piece + 1))

            # 设置高优先级并等待 piece 下载完成
            for p in head_pieces:
                handle.piece_priority(p, 7)
            self.log.debug(f"策略一: 正在下载文件头 ({len(head_pieces)} 个 piece) 以查找 sidx...")
            await self._wait_for_pieces(infohash_hex, handle, head_pieces.copy())
            self.log.debug("文件头下载完成。")

        # 检查是否是 fMP4 并尝试用 sidx 提取
        is_fmp4 = await self.loop.run_in_executor(None, self._is_fmp4, torrent_reader)
        if is_fmp4:
            # _extract_keyframes_fmp4 会使用已下载的数据进行解析
            valid_packet_infos = await self._extract_keyframes_fmp4(handle, video_file_index)
            if valid_packet_infos:
                self.log.info("策略一成功: 通过 fMP4/sidx 方法成功提取到关键帧。")
                return valid_packet_infos, head_pieces, set()

        self.log.info("策略一失败或不适用。转至策略二。")

        # --- 策略二: 尝试作为标准 MP4 使用 moov box 索引 ---
        self.log.info("策略二: 尝试通过文件头/尾的索引 (moov) 提取关键帧...")

        # 下载文件头和尾部各 1MB
        header_size = 1 * 1024 * 1024
        header_size = min(header_size, target_file.size)

        # 计算头部的 pieces (可能部分已在策略一中下载)
        head_pieces_for_moov = set()
        if header_size > 0:
            start_req = ti.map_file(video_file_index, 0, 1)
            end_req = ti.map_file(video_file_index, header_size - 1, 1)
            head_pieces_for_moov = set(range(start_req.piece, end_req.piece + 1))

        # 下载文件尾1MB
        footer_size = 1 * 1024 * 1024
        footer_start = max(0, target_file.size - footer_size)
        foot_pieces = set()
        # 只有当尾部不与头部重叠时才下载
        if footer_start > header_size:
            start_req_foot = ti.map_file(video_file_index, footer_start, 1)
            end_req_foot = ti.map_file(video_file_index, target_file.size - 1, 1)
            foot_pieces = set(range(start_req_foot.piece, end_req_foot.piece + 1))

        # 合并所有需要下载的 pieces，并设置优先级
        pieces_to_download = head_pieces_for_moov.union(foot_pieces)
        # 排除掉在策略一中已经下载过的 piece
        pieces_to_download.difference_update(head_pieces)

        if pieces_to_download:
            self.log.debug(f"策略二: 正在下载额外的头/尾 pieces ({len(pieces_to_download)} 个)...")
            for p in pieces_to_download:
                handle.piece_priority(p, 7)
            await self._wait_for_pieces(infohash_hex, handle, pieces_to_download)
            self.log.debug("头/尾 pieces 下载完成。")

        try:
            # 使用 TorrentFileReader (它会利用已下载的数据)
            valid_packet_infos = await self.loop.run_in_executor(
                None, self._extract_keyframes_from_index, torrent_reader
            )
            if valid_packet_infos:
                self.log.info("策略二成功: 通过 moov 索引成功提取到关键帧。")
                downloaded_pieces = head_pieces.union(head_pieces_for_moov).union(foot_pieces)
                return valid_packet_infos, downloaded_pieces, foot_pieces
            else:
                 self.log.warning("策略二警告: 调用索引提取函数后未返回任何关键帧。")
        except av.error.InvalidDataError:
            self.log.warning("策略二失败: 使用索引提取关键帧时发生 av.error.InvalidDataError，可能元数据不完整。")
            pass

        self.log.error("所有关键帧提取策略均失败。")
        # 返回空结果，表示失败
        return [], set(), set()


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

            # 2. 查找目标视频文件
            ti = handle.get_torrent_info()
            total_pieces = ti.num_pieces()

            # 设置所有 piece 的优先级为 0，防止自动下载。
            handle.prioritize_pieces([0] * total_pieces)
            self.log.debug(f"已将全部 {total_pieces} 个 piece 设置为优先级 0 以节约带宽。")
            video_file_index = -1
            target_file = None
            max_size = -1
            for i, f in enumerate(ti.files()):
                if f.path.lower().endswith(".mp4") and f.size > max_size:
                    max_size = f.size
                    video_file_index = i
                    target_file = f

            if video_file_index == -1:
                self.log.warning(f"在种子 {infohash_hex} 中未找到MP4视频文件。")
                return

            self.log.debug(f"已确定目标视频文件: {target_file.path} (大小: {target_file.size})")

            # 3. 使用新的策略性方法提取关键帧
            valid_packet_infos, head_pieces, foot_pieces = await self._extract_keyframes_strategically(
                handle, video_file_index, infohash_hex
            )

            # 4. 如果两种方法都失败了，则任务失败
            if not valid_packet_infos:
                self.log.error(f"无法为 {infohash_hex} 提取任何有效关键帧。任务中止。")
                return

            # 5. Prioritize all necessary pieces at once
            all_pieces_needed = set()
            pieces_per_keyframe = []
            for keyframe_info, timestamp_str in valid_packet_infos:
                pieces = self._get_pieces_for_packet(ti, video_file_index, keyframe_info)
                pieces_per_keyframe.append(pieces)
                all_pieces_needed.update(pieces)

            # Calculate and log the *required* download percentage
            # This is the theoretical minimum we must download.
            required_pieces = head_pieces.copy()
            if 'foot_pieces' in locals():
                required_pieces.update(foot_pieces)
            required_pieces.update(all_pieces_needed)

            if total_pieces > 0:
                required_percentage = (len(required_pieces) / total_pieces) * 100
                self.log.info(f"必需 piece 占比: {len(required_pieces)} / {total_pieces} 个 pieces ({required_percentage:.2f}%).")

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

            # Calculate and log download statistics
            total_pieces = ti.num_pieces()
            if total_pieces > 0:
                downloaded_pieces = sum(1 for i in range(total_pieces) if handle.have_piece(i))
                percentage = (downloaded_pieces / total_pieces) * 100
                self.log.info(f"下载统计: {downloaded_pieces} / {total_pieces} 个 pieces ({percentage:.2f}%) 已下载。")

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

    def _extract_keyframes_from_index(self, torrent_reader):
        """
        使用视频索引提取关键帧信息（如果可用）。
        这比扫描整个文件快得多，但依赖于视频容器有索引（例如MP4中的'moov' atom）。
        NOTE: 这是一个阻塞函数，应该在执行器中运行。
        """
        self.log.debug("正在尝试从视频索引中提取关键帧...")
        # torrent_reader 的位置可能在中间，需要重置
        torrent_reader.seek(0)
        valid_packet_infos = []

        try:
            with av.open(torrent_reader) as container:
                stream = container.streams.video[0]
                if not stream.index:
                    self.log.warning("Video stream has no index. Cannot use fast keyframe extraction.")
                    return []

                duration_sec = container.duration / 1_000_000.0
                if not duration_sec or duration_sec <= 0:
                    self.log.error("Could not determine a valid video duration.")
                    return []

                if duration_sec > 3600:
                    num_screenshots = int(duration_sec / 180)
                else:
                    num_screenshots = 20

                self.log.debug(f"Video duration: {duration_sec:.2f}s. Index has {len(stream.index)} entries.")

                # We are interested in keyframes, which are a subset of index entries.
                # PyAV's `is_keyframe` on index entries is not reliably exposed.
                # However, seeking to a time with `any_frame=False` finds keyframes.
                # A simpler approach that works if the index is roughly linear is to
                # treat all index entries as potential keyframe candidates and select from them.
                # This is a good balance of performance and simplicity.
                index_entries = list(stream.index)

                # Create a sorted list of presentation timestamps (pts) for efficient searching.
                entry_pts = [e.pts for e in index_entries]

                timestamp_secs = [(duration_sec / (num_screenshots + 1)) * (i + 1) for i in range(num_screenshots)]
                time_base = stream.time_base
                seen_pts = set()

                for ts in timestamp_secs:
                    target_pts = int(ts / time_base)
                    insertion_point = bisect.bisect_right(entry_pts, target_pts)

                    if insertion_point > 0:
                        best_entry_index = insertion_point - 1
                        entry = index_entries[best_entry_index]

                        if entry.pts not in seen_pts:
                            keyframe_info = KeyframeInfo(pts=entry.pts, pos=entry.pos, size=entry.size)
                            m, s = divmod(ts, 60)
                            h, m = divmod(m, 60)
                            timestamp_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"
                            valid_packet_infos.append((keyframe_info, timestamp_str))
                            seen_pts.add(entry.pts)

            self.log.info(f"Successfully extracted {len(valid_packet_infos)} keyframe positions from index.")
            return valid_packet_infos

        except Exception as e:
            self.log.error(f"Could not extract keyframes from index: {e!r}. This might happen if the video metadata (moov atom) is not in the header/footer.", exc_info=True)
            return []

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

    def _get_all_keyframes(self, handle, video_file_index):
        """
        Scans the entire video file to find all keyframes and the time_base.
        This is a slow, blocking operation, but more reliable than seeking by time.
        """
        self.log.debug("Scanning for all keyframes...")
        torrent_reader = TorrentFileReader(self, handle, video_file_index)
        keyframes = []
        try:
            with av.open(torrent_reader) as container:
                stream = container.streams.video[0]
                time_base = stream.time_base
                for packet in container.demux(stream):
                    if packet.dts is not None and packet.is_keyframe:
                        info = KeyframeInfo(pts=packet.pts, pos=packet.pos, size=packet.size)
                        keyframes.append(info)
            self.log.debug(f"Found {len(keyframes)} total keyframes.")
            return keyframes, time_base
        except Exception as e:
            self.log.error(f"Could not scan for keyframes: {e!r}")
            return [], None

    def _get_pieces_for_packet(self, ti, video_file_index, keyframe_info):
        """Calculates the set of pieces required for a given KeyframeInfo."""
        start_req = ti.map_file(video_file_index, keyframe_info.pos, 1)
        # The keyframe_info.size can be 0 sometimes, so read at least 1 byte
        read_size = max(1, keyframe_info.size)
        end_req = ti.map_file(video_file_index, keyframe_info.pos + read_size - 1, 1)
        return set(range(start_req.piece, end_req.piece + 1))

    def _decode_and_save_frame(self, handle, video_file_index, keyframe_info, infohash_hex, timestamp_str):
        """
        解码并保存单个关键帧。
        此函数现在更健壮，优先使用字节位置进行seek，这对于fMP4文件更可靠。
        NOTE: 这是一个阻塞函数。
        """
        self.log.debug(f"开始解码时间戳 {timestamp_str} 的帧 (位置: {keyframe_info.pos}, pts: {keyframe_info.pts})")
        torrent_reader = TorrentFileReader(self, handle, video_file_index)
        try:
            with av.open(torrent_reader) as container:
                stream = container.streams.video[0]
                # 优先使用字节位置进行seek，对于从sidx等box解析出的位置更精确
                # any_frame=True 确保我们能解码找到的第一个帧，它应该就是我们目标的关键帧
                container.seek(keyframe_info.pos, whence='byte', any_frame=True, stream=stream)

                # 解码找到的第一个帧
                for frame in container.decode(stream):
                    output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"
                    os.makedirs(self.output_dir, exist_ok=True)
                    frame.to_image().save(output_filename)
                    self.log.info(f"成功: 截图已保存到 {output_filename}")
                    return

                self.log.error(f"解码失败: 在位置 {keyframe_info.pos} seek后未找到可解码的帧。")

        except Exception as e:
            self.log.error(f"解码时间戳 {timestamp_str} 的帧时发生意外错误: {e!r}", exc_info=True)

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
