# -*- coding: utf-8 -*-
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
    """自定义异常，用于清晰地传递来自 libtorrent 核心的特定错误。"""
    def __init__(self, error_code):
        self.error_code = error_code
        super().__init__(f"Libtorrent error: {error_code.message()}")


# ==============================================================================
# TorrentFileReader: 一个模拟文件行为的 Torrent 读取类
# ==============================================================================
class TorrentFileReader(io.RawIOBase):
    """
    这是一个核心类，它将 libtorrent 的数据块（piece）下载功能封装成一个文件类接口（file-like object）。
    这个类的 read() 方法是同步的，设计为在单独的线程中运行，从而不会阻塞主服务的 asyncio 事件循环。
    """
    def __init__(self, service, handle, file_index):
        super().__init__()
        self.service = service
        self.handle = handle
        self.ti = handle.torrent_file()
        self.file_index = file_index
        self.file_entry = self.ti.file_at(self.file_index)
        self.file_size = self.file_entry.size
        self.pos = 0
        self.piece_cache = OrderedDict()
        self.PIECE_CACHE_SIZE = 32
        self.log = logging.getLogger("TorrentFileReader")

    def readable(self): return True
    def seekable(self): return True

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET: self.pos = offset
        elif whence == io.SEEK_CUR: self.pos += offset
        elif whence == io.SEEK_END: self.pos = self.file_size + offset
        return self.pos

    def tell(self):
        return self.pos

    def read(self, size=-1):
        if size == -1: size = self.file_size - self.pos
        size = min(size, self.file_size - self.pos)
        if size <= 0: return b''

        result_buffer = bytearray(size)
        buffer_offset, bytes_to_go, current_file_pos = 0, size, self.pos
        piece_size = self.ti.piece_length()

        while bytes_to_go > 0:
            req = self.ti.map_file(self.file_index, current_file_pos, 1)
            piece_index, piece_offset = req.piece, req.start
            read_len = min(bytes_to_go, piece_size - piece_offset)

            if piece_index in self.piece_cache:
                piece_data = self.piece_cache[piece_index]
                self.piece_cache.move_to_end(piece_index)
            else:
                coro = self.service.download_and_read_piece(self.handle, piece_index)
                future_result = asyncio.run_coroutine_threadsafe(coro, self.service.loop)
                try:
                    piece_data = future_result.result(timeout=180)
                except Exception as e:
                    self.log.error(f"在主循环中等待 piece {piece_index} 时出错: {e}")
                    raise IOError(f"获取 piece {piece_index} 失败") from e

                self.piece_cache[piece_index] = piece_data
                if len(self.piece_cache) > self.PIECE_CACHE_SIZE:
                    self.piece_cache.popitem(last=False)

            if piece_data is None: raise IOError(f"读取 piece {piece_index} 失败: 未获取到数据。")

            chunk = piece_data[piece_offset : piece_offset + read_len]
            result_buffer[buffer_offset : buffer_offset + len(chunk)] = chunk
            buffer_offset += read_len; bytes_to_go -= read_len; current_file_pos += read_len

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
        if self.alert_task: self.alert_task.cancel()
        for worker in self.workers: worker.cancel()
        if hasattr(self, 'ses'): del self.ses
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
        if not handle.have_piece(piece_index):
            handle.piece_priority(piece_index, 7)
            future = self.loop.create_future()
            self.piece_download_futures[piece_index].append(future)
            await future
        read_future = self.loop.create_future()
        with self.pending_reads_lock:
            self.pending_reads[piece_index].append(read_future)
        handle.read_piece(piece_index)
        return await read_future

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
            video_file_index, max_size = -1, -1
            for i in range(ti.num_files()):
                f = ti.file_at(i)
                if f.path.lower().endswith((".mp4", ".mkv")) and f.size > max_size:
                    max_size, video_file_index = f.size, i
            if video_file_index == -1: self.log.warning(f"在 {infohash_hex} 中未找到视频文件。"); return
            valid_packet_infos = await self.loop.run_in_executor(None, self._extract_keyframes_from_moov, handle, video_file_index)
            if not valid_packet_infos: self.log.error(f"无法为 {infohash_hex} 提取任何有效关键帧。任务中止。"); return
            decode_tasks = [self.loop.run_in_executor(None, self._decode_and_save_frame, handle, video_file_index, info, ts) for info, ts in valid_packet_infos]
            await asyncio.gather(*decode_tasks)
            self.log.info(f"{infohash_hex} 的截图任务完成。")
        except asyncio.TimeoutError: self.log.error(f"处理 {infohash_hex} 时发生超时。")
        except Exception: self.log.exception(f"处理 {infohash_hex} 时发生未知错误。")
        finally:
            if infohash_hex in self.pending_metadata: self.pending_metadata.pop(infohash_hex, None)
            if handle: self.ses.remove_torrent(handle, lt.session.delete_files)

    def _extract_keyframes_from_moov(self, handle, video_file_index):
        self.log.debug("正在尝试从 'moov' box 索引中手动提取关键帧...")
        ti = handle.torrent_file()
        target_file = ti.file_at(video_file_index)
        torrent_reader = TorrentFileReader(self, handle, video_file_index)
        read_size = min(5 * 1024 * 1024, target_file.size)
        torrent_reader.seek(0)
        file_data = torrent_reader.read(read_size)
        from .pymp4parse import F4VParser
        moov_box = next((b for b in F4VParser.parse(bytes_input=file_data) if b.header.box_type == 'moov'), None)
        if not moov_box:
            torrent_reader.seek(max(0, target_file.size - read_size))
            file_data = torrent_reader.read(read_size)
            moov_box = next((b for b in F4VParser.parse(bytes_input=file_data) if b.header.box_type == 'moov'), None)

        if not moov_box: self.log.error("在已下载的数据中未找到 'moov' box。"); return []

        stbl_box = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'minf', 'stbl'])
        if not stbl_box: return []
        stss, stts, stsc, stsz, stco, co64 = (getattr(stbl_box, attr, None) for attr in ['stss', 'stts', 'stsc', 'stsz', 'stco', 'co64'])
        if not all([stss, stts, stsc, stsz, (stco or co64)]): return []
        keyframe_samples, chunk_offsets = stss.entries, stco.entries if stco else co64.entries
        sample_sizes = stsz.entries if stsz.sample_size == 0 else [stsz.sample_size] * stsz.sample_count
        sample_timestamps = []; current_time = 0
        for count, duration in stts.entries:
            for _ in range(count): sample_timestamps.append(current_time); current_time += duration

        # 恢复完整、正确的 sample-to-offset 计算逻辑
        sample_offsets = []
        stsc_entries_iter = iter(stsc.entries)
        chunk_offsets_iter = iter(chunk_offsets)
        current_stsc = next(stsc_entries_iter, None)
        next_stsc = next(stsc_entries_iter, None)
        current_chunk_num = 1
        current_sample_in_chunk = 0
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

        all_keyframes = []
        for s_num in keyframe_samples:
            idx = s_num - 1
            if idx < len(sample_offsets) and idx < len(sample_timestamps) and idx < len(sample_sizes):
                all_keyframes.append(KeyframeInfo(sample_timestamps[idx], sample_offsets[idx], sample_sizes[idx]))
        if not all_keyframes: return []
        tkhd = F4VParser.find_child_box(moov_box, ['trak', 'tkhd']); mdhd = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'mdhd'])
        timescale = mdhd.timescale if mdhd else 1000
        duration = tkhd.duration if tkhd else sum(c * d for c, d in stts.entries)
        duration_sec = duration / timescale if timescale else 0
        num_screenshots = 20 if duration_sec <= 3600 else int(duration_sec / 180)
        selected_keyframes = all_keyframes if len(all_keyframes) <= num_screenshots else [all_keyframes[int(i * len(all_keyframes) / num_screenshots)] for i in range(num_screenshots)]
        valid_infos = []
        for info in selected_keyframes:
            ts_sec = info.pts / timescale if timescale else 0
            m, s = divmod(ts_sec, 60); h, m = divmod(m, 60)
            valid_infos.append((info, f"{int(h):02d}:{int(m):02d}:{int(round(s)):02d}"))
        return valid_infos

    def _get_pieces_for_packet(self, ti, video_file_index, keyframe_info):
        start_req = ti.map_file(video_file_index, keyframe_info.pos, 1)
        read_size = max(1, keyframe_info.size)
        end_req = ti.map_file(video_file_index, keyframe_info.pos + read_size - 1, 1)
        return set(range(start_req.piece, end_req.piece + 1))

    def _decode_and_save_frame(self, handle, video_file_index, keyframe_info, infohash_hex, timestamp_str):
        self.log.debug(f"开始解码时间戳 {timestamp_str} 的帧 (位置: {keyframe_info.pos})")
        torrent_reader = TorrentFileReader(self, handle, video_file_index)
        try:
            pieces = self._get_pieces_for_packet(handle.torrent_file(), video_file_index, keyframe_info)
            for p in pieces: handle.piece_priority(p, 7)
            torrent_reader.seek(keyframe_info.pos)
            read_size = keyframe_info.size + 512 * 1024
            keyframe_data = torrent_reader.read(read_size)
            if not keyframe_data: self.log.error(f"解码失败: 从位置 {keyframe_info.pos} 读取数据失败。"); return
            with av.open(io.BytesIO(keyframe_data)) as container:
                for frame in container.decode(video=0):
                    output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"
                    os.makedirs(self.output_dir, exist_ok=True)
                    frame.to_image().save(output_filename)
                    self.log.info(f"成功: 截图已保存到 {output_filename}"); return
                self.log.error(f"解码失败: 在数据块中未找到可解码的视频帧 (位置: {keyframe_info.pos})。")
        except Exception:
            self.log.exception(f"解码时间戳 {timestamp_str} 的帧时发生意外错误。")

    async def _worker(self):
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError: break
            except Exception: self.log.exception("截图工作者发生错误。")
