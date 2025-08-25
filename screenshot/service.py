# -*- coding: utf-8 -*-
import asyncio
import bisect
import logging
import binascii
import os
import io
import struct
import time
import av
import libtorrent as lt
import threading
from collections import namedtuple, defaultdict, OrderedDict
from concurrent.futures import Future

KeyframeInfo = namedtuple('KeyframeInfo', ['pts', 'pos', 'size'])


class LibtorrentError(Exception):
    """自定义异常，用于清晰地传递来自 libtorrent 核心的特定错误。"""
    def __init__(self, error_code):
        self.error_code = error_code
        super().__init__(f"Libtorrent error: {error_code.message()}")


# ==============================================================================
# AsyncTorrentReader: 一个异步的 Torrent 读取类
# ==============================================================================
class AsyncTorrentReader:
    """
    这是一个完全异步的读取器，它将 libtorrent 的数据块（piece）下载功能封装成一个异步的、
    类似文件的接口。所有的 I/O 操作都是非阻塞的。
    """
    def __init__(self, service, handle, file_index):
        self.service = service
        self.handle = handle
        self.ti = handle.torrent_file()
        self.file_index = file_index
        # 使用现代 API files() 替换已弃用的 file_at()
        file_storage = self.ti.files()
        self.file_size = file_storage.file_size(self.file_index)
        self.pos = 0
        self.piece_cache = OrderedDict()
        self.PIECE_CACHE_SIZE = 32 # 缓存最近使用的 piece
        self.log = logging.getLogger("AsyncTorrentReader")

    def seek(self, offset, whence=io.SEEK_SET):
        """同步方法，用于设置读取指针的位置。"""
        if whence == io.SEEK_SET: self.pos = offset
        elif whence == io.SEEK_CUR: self.pos += offset
        elif whence == io.SEEK_END: self.pos = self.file_size + offset
        return self.pos

    def tell(self):
        """同步方法，返回当前读取指针的位置。"""
        return self.pos

    async def read(self, size=-1):
        """
        异步方法，从 torrent 文件中读取指定大小的数据。
        这个方法会按需触发 piece 的下载和读取，但所有操作都是通过 await 完成的，
        不会阻塞事件循环。
        """
        if size == -1: size = self.file_size - self.pos
        size = min(size, self.file_size - self.pos)
        if size <= 0: return b''

        result_buffer = bytearray(size)
        buffer_offset, bytes_to_go, current_file_pos = 0, size, self.pos
        piece_size = self.ti.piece_length()

        while bytes_to_go > 0:
            # 将文件偏移映射到 piece 索引和 piece 内的偏移
            req = self.ti.map_file(self.file_index, current_file_pos, 1)
            piece_index, piece_offset = req.piece, req.start
            # 计算本次循环需要读取的字节数
            read_len = min(bytes_to_go, piece_size - piece_offset)

            # 检查缓存
            if piece_index in self.piece_cache:
                piece_data = self.piece_cache[piece_index]
                self.piece_cache.move_to_end(piece_index)
            else:
                # 如果不在缓存中，则异步下载并读取 piece
                try:
                    piece_data = await self.service.download_and_read_piece(self.handle, piece_index)
                except Exception as e:
                    self.log.error(f"异步读取 piece {piece_index} 时出错: {e}")
                    raise IOError(f"获取 piece {piece_index} 失败") from e

                # 更新缓存
                self.piece_cache[piece_index] = piece_data
                if len(self.piece_cache) > self.PIECE_CACHE_SIZE:
                    self.piece_cache.popitem(last=False)

            if piece_data is None:
                raise IOError(f"读取 piece {piece_index} 失败: 未获取到数据。")

            # 从 piece 数据中提取所需部分
            chunk = piece_data[piece_offset : piece_offset + read_len]
            result_buffer[buffer_offset : buffer_offset + len(chunk)] = chunk

            # 更新计数器
            buffer_offset += read_len
            bytes_to_go -= read_len
            current_file_pos += read_len

        self.pos += size
        return bytes(result_buffer)

# ==============================================================================
# ScreenshotService: 主服务类
# ==============================================================================
class ScreenshotService:
    def __init__(self, loop=None, num_workers=10, output_dir='./screenshots_output'):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotService")
        settings = {
            'listen_interfaces': '0.0.0.0:6881', 'enable_dht': True,
            'alert_mask': lt.alert_category.error | lt.alert_category.status | lt.alert_category.storage,
            'dht_bootstrap_nodes': 'router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881',
        }
        self.ses = lt.session(settings)
        self.task_queue = asyncio.Queue()
        self.workers = []
        self.alert_task = None
        self._running = False
        self.dht_ready = asyncio.Event()
        self.pending_metadata = {}
        self.pending_reads = defaultdict(list)
        self.pending_reads_lock = threading.Lock()
        self.piece_download_futures = defaultdict(list)

    async def run(self):
        self.log.info("正在启动截图服务...")
        self._running = True
        self.alert_task = self.loop.create_task(self._alert_loop())
        for _ in range(self.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info(f"截图服务已启动，拥有 {self.num_workers} 个工作者。")

    def stop(self):
        self.log.info("正在停止截图服务...")
        self._running = False
        if self.alert_task:
            self.alert_task.cancel()
        for worker in self.workers:
            worker.cancel()
        # self.ses 对象由 libtorrent 管理，不需要手动删除
        # 在服务停止后，让垃圾回收器自然处理即可
        self.log.info("截图服务已停止。")

    async def submit_task(self, infohash=None, torrent_file_content=None):
        """提交一个新任务，可以通过 infohash 或 torrent 文件内容。"""
        if infohash and not torrent_file_content:
            await self.task_queue.put({'infohash': infohash})
            self.log.info(f"已提交新任务 (infohash): {infohash}")
        elif torrent_file_content:
            ti = lt.torrent_info(torrent_file_content)
            infohash = str(ti.info_hashes().v1)
            await self.task_queue.put({'infohash': infohash, 'ti_content': torrent_file_content})
            self.log.info(f"已提交新任务 (torrent file): {infohash}")

    async def download_and_read_piece(self, handle, piece_index):
        """
        健壮的、异步的 piece 读取方法。
        此方法处理了 piece 可能在等待前就已经下载完成的竞态条件。
        """
        # 如果 piece 已经存在，直接进入读取阶段
        if not handle.have_piece(piece_index):
            self.log.debug(f"Piece {piece_index}: 不存在，设置高优先级并等待下载。")
            handle.piece_priority(piece_index, 7)
            future = self.loop.create_future()
            self.piece_download_futures[piece_index].append(future)

            # 增加一个超时以防止无限等待
            try:
                await asyncio.wait_for(future, timeout=60.0)
            except asyncio.TimeoutError:
                self.log.error(f"Piece {piece_index}: 等待下载超时。")
                # 检查 piece 是否最终还是下载好了，处理竞态条件
                if not handle.have_piece(piece_index):
                    raise LibtorrentError("Piece download timed out and piece not available.")

        # piece 已下载，现在读取它
        read_future = self.loop.create_future()
        with self.pending_reads_lock:
            self.pending_reads[piece_index].append(read_future)
        handle.read_piece(piece_index)

        try:
            return await asyncio.wait_for(read_future, timeout=60.0)
        except asyncio.TimeoutError:
            self.log.error(f"Piece {piece_index}: 读取操作超时。")
            raise LibtorrentError("Piece read timed out.")

    def _handle_metadata_received(self, alert):
        infohash_str = str(alert.handle.info_hash())
        if infohash_str in self.pending_metadata and not self.pending_metadata[infohash_str].done():
            self.pending_metadata[infohash_str].set_result(alert.handle)

    def _handle_piece_finished(self, alert):
        futures = self.piece_download_futures.pop(alert.piece_index, [])
        for future in futures:
            if not future.done(): future.set_result(True)

    def _handle_dht_bootstrap(self, alert):
        if not self.dht_ready.is_set(): self.dht_ready.set()

    def _handle_read_piece(self, alert):
        with self.pending_reads_lock:
            futures = self.pending_reads.pop(alert.piece, [])
        if alert.error and alert.error.value() != 0:
            error = LibtorrentError(alert.error)
            for future in futures:
                if not future.done(): future.set_exception(error)
            return
        data = bytes(alert.buffer)
        for future in futures:
            if not future.done(): future.set_result(data)

    async def _alert_loop(self):
        while self._running:
            try:
                alerts = self.ses.pop_alerts()
                for alert in alerts:
                    if alert.category() & lt.alert_category.error: self.log.error(f"Libtorrent Alert: {alert}")
                    else: self.log.debug(f"Libtorrent Alert: {alert}")
                    if isinstance(alert, lt.metadata_received_alert): self._handle_metadata_received(alert)
                    elif isinstance(alert, lt.piece_finished_alert): self._handle_piece_finished(alert)
                    elif isinstance(alert, lt.read_piece_alert): self._handle_read_piece(alert)
                    elif isinstance(alert, lt.dht_bootstrap_alert): self._handle_dht_bootstrap(alert)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError: break
            except Exception: self.log.exception("libtorrent alert 循环发生错误。")

    async def _handle_screenshot_task(self, task_info):
        infohash_hex = task_info['infohash']
        self.log.info(f"开始处理任务: {infohash_hex}")
        handle = None
        save_dir = f"/dev/shm/{infohash_hex}"
        os.makedirs(save_dir, exist_ok=True)
        try:
            if 'ti_content' in task_info:
                ti = lt.torrent_info(task_info['ti_content'])
                params = {'ti': ti, 'save_path': save_dir}
            else:
                meta_future = self.loop.create_future()
                self.pending_metadata[infohash_hex] = meta_future
                trackers = [
                    "udp://tracker.openbittorrent.com:80", "udp://tracker.opentrackr.org:1337/announce",
                    "udp://tracker.coppersurfer.tk:6969/announce", "udp://tracker.leechers-paradise.org:6969/announce",
                ]
                magnet_uri = f"magnet:?xt=urn:btih:{infohash_hex}&{'&'.join(['tr=' + t for t in trackers])}"
                params = lt.parse_magnet_uri(magnet_uri)
                params.save_path = save_dir

            handle = self.ses.add_torrent(params)

            if 'ti_content' not in task_info:
                await self.dht_ready.wait()
                self.log.debug("等待元数据...")
                handle = await asyncio.wait_for(meta_future, timeout=180)

            ti = handle.torrent_file()
            if not ti: self.log.error(f"未能获取 {infohash_hex} 的 torrent_file 对象。"); return
            handle.prioritize_pieces([0] * ti.num_pieces())

            # 使用现代 API files() 替换已弃用的 file_at()
            files = ti.files()
            video_file_index, max_size = -1, -1
            for i in range(files.num_files()):
                if files.file_path(i).lower().endswith((".mp4", ".mkv")) and files.file_size(i) > max_size:
                    max_size = files.file_size(i)
                    video_file_index = i

            if video_file_index == -1: self.log.warning(f"在 {infohash_hex} 中未找到视频文件。"); return

            # 从 _extract_keyframes_from_moov 中分离出 CPU 密集型解析部分
            def parse_moov_sync(moov_data):
                from .pymp4parse import F4VParser
                moov_box = next((b for b in F4VParser.parse(bytes_input=moov_data) if b.header.box_type == 'moov'), None)
                if not moov_box: return None
                return self._parse_keyframes_from_stbl(moov_box)

            moov_data = await self._read_moov_data(handle, video_file_index)
            if not moov_data:
                self.log.error(f"无法为 {infohash_hex} 读取 'moov' 数据。")
                return

            valid_packet_infos = await self.loop.run_in_executor(None, parse_moov_sync, moov_data)
            if not valid_packet_infos:
                self.log.error(f"无法为 {infohash_hex} 提取任何有效关键帧。任务中止。")
                return

            # 直接创建异步任务列表
            decode_tasks = [
                self._decode_and_save_frame(handle, video_file_index, info, infohash_hex, ts)
                for info, ts in valid_packet_infos
            ]
            await asyncio.gather(*decode_tasks)
            self.log.info(f"{infohash_hex} 的截图任务完成。")
        except asyncio.TimeoutError: self.log.error(f"处理 {infohash_hex} 时发生超时。")
        except Exception: self.log.exception(f"处理 {infohash_hex} 时发生未知错误。")
        finally:
            if infohash_hex in self.pending_metadata: self.pending_metadata.pop(infohash_hex, None)
            if handle: self.ses.remove_torrent(handle, lt.session.delete_files)

    async def _read_moov_data(self, handle, video_file_index):
        """异步地从 torrent 中读取 'moov' box 的原始数据。"""
        self.log.debug("正在异步读取 'moov' box 数据...")
        ti = handle.torrent_file()
        files = ti.files()
        target_file_size = files.file_size(video_file_index)

        torrent_reader = AsyncTorrentReader(self, handle, video_file_index)
        read_size = min(5 * 1024 * 1024, target_file_size)

        # 尝试从文件头部读取
        torrent_reader.seek(0)
        file_data = await torrent_reader.read(read_size)
        if b'moov' in file_data:
            return file_data

        # 尝试从文件尾部读取
        self.log.debug("'moov' box 未在文件头部找到，尝试从尾部读取...")
        torrent_reader.seek(max(0, target_file_size - read_size))
        file_data = await torrent_reader.read(read_size)
        if b'moov' in file_data:
            return file_data

        return None

    def _parse_keyframes_from_stbl(self, moov_box):
        """从解析出的 'moov' box 中提取并计算关键帧信息（同步 CPU 密集型操作）。"""
        from .pymp4parse import F4VParser
        stbl_box = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'minf', 'stbl'])
        if not stbl_box: return []

        stss, stts, stsc, stsz, stco, co64 = (getattr(stbl_box, attr, None) for attr in
                                             ['stss', 'stts', 'stsc', 'stsz', 'stco', 'co64'])
        if not all([stss, stts, stsc, stsz, (stco or co64)]): return []

        keyframe_samples = stss.entries
        chunk_offsets = stco.entries if stco else co64.entries
        sample_sizes = stsz.entries if stsz.sample_size == 0 else [stsz.sample_size] * stsz.sample_count

        sample_timestamps = []
        current_time = 0
        for count, duration in stts.entries:
            for _ in range(count):
                sample_timestamps.append(current_time)
                current_time += duration

        sample_offsets = []
        stsc_entries_iter = iter(stsc.entries)
        chunk_offsets_iter = iter(chunk_offsets)
        current_stsc = next(stsc_entries_iter, None)
        next_stsc = next(stsc_entries_iter, None)
        current_chunk_num = 1
        current_sample_in_chunk = 0
        try:
            current_chunk_offset = next(chunk_offsets_iter)
            for sample_size in sample_sizes:
                if current_stsc and (next_stsc is None or current_chunk_num < next_stsc[0]):
                    if current_sample_in_chunk >= current_stsc[1]:
                        current_chunk_num += 1
                        current_sample_in_chunk = 0
                        current_chunk_offset = next(chunk_offsets_iter)
                elif current_stsc and next_stsc and current_chunk_num >= next_stsc[0]:
                    current_stsc = next_stsc
                    next_stsc = next(stsc_entries_iter, None)
                sample_offsets.append(current_chunk_offset)
                current_chunk_offset += sample_size
                current_sample_in_chunk += 1
        except StopIteration:
            self.log.warning("在计算样本偏移时，块偏移或 SC2 数据提前结束。")
            pass

        all_keyframes = []
        for s_num in keyframe_samples:
            idx = s_num - 1
            if idx < len(sample_offsets) and idx < len(sample_timestamps) and idx < len(sample_sizes):
                all_keyframes.append(KeyframeInfo(sample_timestamps[idx], sample_offsets[idx], sample_sizes[idx]))

        if not all_keyframes: return []

        tkhd = F4VParser.find_child_box(moov_box, ['trak', 'tkhd'])
        mdhd = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'mdhd'])
        timescale = mdhd.timescale if mdhd and mdhd.timescale > 0 else 1000
        duration = tkhd.duration if tkhd else sum(c * d for c, d in stts.entries)
        duration_sec = duration / timescale

        num_screenshots = 20 if duration_sec <= 3600 else int(duration_sec / 180)
        selected_keyframes = all_keyframes if len(all_keyframes) <= num_screenshots else \
                             [all_keyframes[int(i * len(all_keyframes) / num_screenshots)] for i in range(num_screenshots)]

        valid_infos = []
        for info in selected_keyframes:
            ts_sec = info.pts / timescale
            m, s = divmod(ts_sec, 60)
            h, m = divmod(m, 60)
            valid_infos.append((info, f"{int(h):02d}:{int(m):02d}:{int(round(s)):02d}"))
        return valid_infos

    def _get_pieces_for_packet(self, ti, video_file_index, keyframe_info):
        start_req = ti.map_file(video_file_index, keyframe_info.pos, 1)
        read_size = max(1, keyframe_info.size)
        end_req = ti.map_file(video_file_index, keyframe_info.pos + read_size - 1, 1)
        return set(range(start_req.piece, end_req.piece + 1))

    def _save_frame_to_jpeg(self, frame, infohash_hex, timestamp_str):
        """同步辅助函数，用于将解码后的帧保存为 JPG 文件。"""
        output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"
        os.makedirs(self.output_dir, exist_ok=True)
        frame.to_image().save(output_filename)
        self.log.info(f"成功: 截图已保存到 {output_filename}")

    async def _decode_and_save_frame(self, handle, video_file_index, keyframe_info, infohash_hex, timestamp_str):
        """
        异步地读取关键帧数据，然后在执行器中同步地解码和保存帧，以避免阻塞事件循环。
        """
        self.log.debug(f"开始解码时间戳 {timestamp_str} 的帧 (位置: {keyframe_info.pos})")

        torrent_reader = AsyncTorrentReader(self, handle, video_file_index)
        try:
            pieces = self._get_pieces_for_packet(handle.torrent_file(), video_file_index, keyframe_info)
            for p in pieces: handle.piece_priority(p, 7)

            torrent_reader.seek(keyframe_info.pos)
            read_size = keyframe_info.size + 512 * 1024
            keyframe_data = await torrent_reader.read(read_size)
            if not keyframe_data:
                self.log.error(f"解码失败: 从位置 {keyframe_info.pos} 读取数据失败。")
                return

            def decode_and_save_sync():
                """将解码和文件写入的同步代码包装在一个函数中。"""
                try:
                    with av.open(io.BytesIO(keyframe_data), 'r') as container:
                        frame = next(container.decode(video=0))
                        self._save_frame_to_jpeg(frame, infohash_hex, timestamp_str)
                except Exception as e:
                    self.log.exception(f"帧 {timestamp_str}: 同步解码/保存函数内部发生错误: {e}")

            await self.loop.run_in_executor(None, decode_and_save_sync)

        except Exception:
            self.log.exception(f"异步解码时间戳 {timestamp_str} 的帧时发生意外错误。")

    async def _worker(self):
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("截图工作者发生错误。")
