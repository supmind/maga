# -*- coding: utf-8 -*-
import asyncio
import logging
import io
import struct
from typing import Generator, Tuple, Optional, Callable, Awaitable, Any
from collections import defaultdict

import av

from .client import TorrentClient, TorrentClientError
from .errors import (
    TaskError,
    NoVideoFileError,
    MoovNotFoundError,
    MoovFetchError,
    MoovParsingError,
    MetadataTimeoutError,
    FrameDownloadTimeoutError,
    FrameDecodeError
)
from .extractor import KeyframeExtractor, Keyframe, SampleInfo
from .generator import ScreenshotGenerator


# 为 status_callback 定义一个类型签名，以增强可读性和静态检查能力
StatusCallback = Callable[..., Awaitable[None]]


from .config import Settings

class ScreenshotService:
    """协调截图生成过程的核心服务类。"""
    def __init__(self, settings: Settings, loop=None, client=None, status_callback: Optional[StatusCallback] = None):
        self.loop = loop or asyncio.get_event_loop()
        self.settings = settings
        self.log = logging.getLogger("ScreenshotService")
        self.task_queue = asyncio.Queue()
        self.workers = []
        self._running = False

        self.client = client or TorrentClient(
            loop=self.loop,
            save_path=self.settings.torrent_save_path,
            metadata_timeout=self.settings.metadata_timeout
        )
        self.generator = ScreenshotGenerator(loop=self.loop, output_dir=self.settings.output_dir)
        self.status_callback = status_callback
        self.active_tasks = set()
        self._submit_lock = asyncio.Lock()

    async def run(self):
        """启动服务，包括 torrent 客户端和工作进程。"""
        self.log.info("正在启动 ScreenshotService...")
        self._running = True
        await self.client.start()
        for _ in range(self.settings.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info("ScreenshotService 已启动，拥有 %d 个工作进程。", self.settings.num_workers)

    async def stop(self):
        """异步地停止服务，包括 torrent 客户端和工作进程。"""
        self.log.info("正在停止 ScreenshotService...")
        self._running = False
        # 等待客户端完全停止
        await self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("ScreenshotService 已停止。")

    async def submit_task(self, infohash: str, metadata: bytes = None, resume_data: dict = None):
        """
        提交一个新的截图任务，并防止重复提交。
        """
        # 使用锁来确保检查和添加操作的原子性，防止竞态条件
        async with self._submit_lock:
            if infohash in self.active_tasks:
                self.log.warning("任务 %s 已在处理中，本次提交被忽略。", infohash)
                return
            self.active_tasks.add(infohash)

        await self.task_queue.put({'infohash': infohash, 'metadata': metadata, 'resume_data': resume_data})
        if resume_data:
            self.log.info("为 infohash: %s 重新提交了任务", infohash)
        else:
            self.log.info("为 infohash: %s 提交了新任务", infohash)

    def _get_pieces_for_range(self, offset_in_torrent, size, piece_length):
        """为 torrent 中的给定字节范围计算 piece 索引。"""
        if size <= 0: return []
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length
        return list(range(start_piece, end_piece + 1))

    def _assemble_data_from_pieces(self, pieces_data, offset_in_torrent, size, piece_length):
        """从 piece 数据字典中组装一个字节范围。"""
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length

        # Bug 修复：在拼装前，首先检查所有需要的数据块是否都存在。
        # 如果有任何一个缺失，我们无法构成一个连续的、有效的数据块。
        # 在这种情况下，返回一个空字节串来向上游发出失败信号。
        for piece_index in range(start_piece, end_piece + 1):
            if piece_index not in pieces_data:
                self.log.warning("组装数据时缺少 piece #%d，操作中止。", piece_index)
                return b""

        # 确认所有数据块都存在后，再进行拼装。
        buffer = bytearray(size)
        buffer_offset = 0
        for piece_index in range(start_piece, end_piece + 1):
            piece_data = pieces_data[piece_index]
            copy_from_start = 0
            if piece_index == start_piece:
                copy_from_start = offset_in_torrent % piece_length
            copy_to_end = piece_length
            if piece_index == end_piece:
                copy_to_end = (offset_in_torrent + size - 1) % piece_length + 1
            chunk = piece_data[copy_from_start:copy_to_end]
            bytes_to_copy = len(chunk)
            if (buffer_offset + bytes_to_copy) > size:
                bytes_to_copy = size - buffer_offset
            if bytes_to_copy > 0:
                buffer[buffer_offset : buffer_offset + bytes_to_copy] = chunk[:bytes_to_copy]
                buffer_offset += bytes_to_copy
        return bytes(buffer)

    def _parse_mp4_boxes(self, stream: io.BytesIO) -> Generator[Tuple[str, bytes, int, int], None, None]:
        """一个健壮的 MP4 box 解析器，产生 box 类型、其完整数据、偏移量和大小。"""
        stream_buffer = stream.getbuffer()
        buffer_size = len(stream_buffer)
        current_offset = stream.tell()
        while current_offset <= buffer_size - 8:
            stream.seek(current_offset)
            try:
                header_data = stream.read(8)
                if not header_data or len(header_data) < 8: break
                size, box_type_bytes = struct.unpack('>I4s', header_data)
                box_type = box_type_bytes.decode('ascii', 'ignore')
            except struct.error:
                self.log.warning("无法在偏移量 %d 处解包 box 头部。数据不完整。", current_offset)
                break
            box_header_size = 8
            if size == 1:
                if current_offset + 16 > buffer_size:
                    self.log.warning("Box '%s' 有一个 64 位的大小，但没有足够的数据用于头部。", box_type)
                    return # Stop parsing if we can't read the full header
                size_64_data = stream.read(8)
                size = struct.unpack('>Q', size_64_data)[0]
                box_header_size = 16
            elif size == 0:
                size = buffer_size - current_offset
            if size < box_header_size:
                self.log.warning("Box '%s' 的大小无效 %d。停止解析。", box_type, size)
                return # Stop parsing on invalid size
            if current_offset + size > buffer_size:
                self.log.warning("大小为 %d 的 Box '%s' 超出了可用数据范围。无法完全解析。", size, box_type)
                # Yield the partial box and stop, as we can't know where the next box begins.
                partial_box_data = stream_buffer[current_offset:buffer_size]
                yield box_type, bytes(partial_box_data), current_offset, size
                return
            full_box_data = stream_buffer[current_offset:current_offset + size]
            yield box_type, bytes(full_box_data), current_offset, size
            current_offset += size

    async def _get_moov_atom_data(self, handle, video_file_offset, video_file_size, piece_length, infohash_hex) -> bytes:
        """智能地查找并获取 moov atom 数据。"""
        self.log.info("正在智能搜索 moov atom...")
        mdat_size_from_head = 0
        try:
            self.log.info("阶段 1: 探测文件头部以查找 moov atom...")
            initial_probe_size = 256 * 1024
            head_size = min(initial_probe_size, video_file_size)
            if head_size > 0:
                head_pieces = self._get_pieces_for_range(video_file_offset, head_size, piece_length)
                head_data_pieces = await self.client.fetch_pieces(handle, head_pieces, timeout=self.settings.moov_probe_timeout)
                head_data = self._assemble_data_from_pieces(head_data_pieces, video_file_offset, head_size, piece_length)
                stream = io.BytesIO(head_data)
                for box_type, partial_box_data, box_offset, box_size in self._parse_mp4_boxes(stream):
                    self.log.info("在头部发现 box '%s'，偏移量 %d，大小 %d。", box_type, box_offset, box_size)
                    if box_type == 'moov':
                        self.log.info("在头部探测中发现 'moov' atom。")
                        if len(partial_box_data) < box_size:
                            self.log.info("探测获得了部分 moov (%d/%d 字节)。正在获取完整数据。", len(partial_box_data), box_size)
                            full_moov_offset_in_torrent = video_file_offset + box_offset
                            needed_pieces = self._get_pieces_for_range(full_moov_offset_in_torrent, box_size, piece_length)
                            moov_data_pieces = await self.client.fetch_pieces(handle, needed_pieces, timeout=self.settings.moov_probe_timeout)
                            return self._assemble_data_from_pieces(moov_data_pieces, full_moov_offset_in_torrent, box_size, piece_length)
                        else:
                            return partial_box_data
                    if box_type == 'mdat':
                        self.log.info("在头部发现 'mdat' box。假设 'moov' 在尾部。")
                        mdat_size_from_head = box_size
                        break # Stop head probing if mdat is found
        except TorrentClientError as e:
            self.log.error("在头部探测期间发生 torrent 客户端错误: %s", e)
            raise MoovFetchError(f"在 moov 头部探测期间发生 torrent 客户端错误: {e}", infohash_hex) from e
        try:
            self.log.info("阶段 2: Moov 不在头部，探测文件尾部。")
            tail_probe_size = (video_file_size - mdat_size_from_head) if mdat_size_from_head > 0 else (10 * 1024 * 1024)
            tail_file_offset = max(0, video_file_size - tail_probe_size)
            tail_torrent_offset = video_file_offset + tail_file_offset
            tail_size = min(tail_probe_size, video_file_size - tail_file_offset)
            if tail_size > 0:
                tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)
                tail_data_pieces = await self.client.fetch_pieces(handle, tail_pieces, timeout=self.settings.moov_probe_timeout)
                tail_data = self._assemble_data_from_pieces(tail_data_pieces, tail_torrent_offset, tail_size, piece_length)

                # "Find + Validate" strategy
                search_pos = len(tail_data)
                while search_pos > 4:
                    found_pos = tail_data.rfind(b'moov', 0, search_pos)
                    if found_pos == -1: break

                    potential_start_pos = found_pos - 4
                    if potential_start_pos < 0:
                        search_pos = found_pos
                        continue

                    stream = io.BytesIO(tail_data)
                    stream.seek(potential_start_pos)

                    # Validate the potential box
                    try:
                        box_size, box_type_bytes = struct.unpack('>I4s', stream.read(8))
                        if box_type_bytes == b'moov' and box_size > 8 and (potential_start_pos + box_size <= len(tail_data)):
                             # It's a plausible moov box, now fully parse it
                            stream.seek(potential_start_pos)
                            parsed_box_type, full_box_data, box_offset, parsed_box_size = next(self._parse_mp4_boxes(stream), (None, None, None, None))
                            if parsed_box_type == 'moov':
                                self.log.info("成功从尾部解析 'moov' atom，大小为 %d。", len(full_box_data))
                                if len(full_box_data) < parsed_box_size:
                                    moov_offset_in_file = tail_file_offset + box_offset
                                    full_moov_offset_in_torrent = video_file_offset + moov_offset_in_file
                                    needed_pieces = self._get_pieces_for_range(full_moov_offset_in_torrent, parsed_box_size, piece_length)
                                    moov_data_pieces = await self.client.fetch_pieces(handle, needed_pieces, timeout=self.settings.moov_probe_timeout)
                                    return self._assemble_data_from_pieces(moov_data_pieces, full_moov_offset_in_torrent, parsed_box_size, piece_length)
                                else:
                                    return full_box_data
                    except struct.error:
                        pass # Not a valid box, continue searching

                    search_pos = found_pos
        except TorrentClientError as e:
            self.log.error("在尾部探测期间发生 torrent 客户端错误: %s", e)
            raise MoovFetchError(f"在 moov 尾部探测期间发生 torrent 客户端错误: {e}", infohash_hex) from e
        raise MoovNotFoundError("无法在文件的第一部分/最后一部分定位 'moov' atom。", infohash_hex)

    def _find_video_file(self, ti: "lt.torrent_info") -> Tuple[int, int, int]:
        """在 torrent 中查找最大的 .mp4 文件并返回其信息。"""
        video_file_index, video_file_size, video_file_offset = -1, -1, -1
        fs = ti.files()
        for i in range(fs.num_files()):
            if fs.file_path(i).lower().endswith('.mp4') and fs.file_size(i) > video_file_size:
                video_file_size = fs.file_size(i)
                video_file_index = i
                video_file_offset = fs.file_offset(i)
        return video_file_index, video_file_size, video_file_offset

    def _select_keyframes(self, all_keyframes: list, timescale: int, samples: list) -> list:
        """根据一系列规则，从所有关键帧中选择一个有代表性的子集用于生成截图。"""
        if not all_keyframes: return []
        duration_sec = samples[-1].pts / timescale if timescale > 0 else 0

        # 使用可配置的参数来决定截图数量
        if duration_sec > 0:
            num_screenshots = max(self.settings.min_screenshots, min(int(duration_sec / self.settings.target_interval_sec), self.settings.max_screenshots))
        else:
            num_screenshots = self.settings.default_screenshots

        if len(all_keyframes) <= num_screenshots:
            return all_keyframes
        else:
            indices = [int(i * len(all_keyframes) / num_screenshots) for i in range(num_screenshots)]
            return [all_keyframes[i] for i in sorted(list(set(indices)))]

    def _serialize_task_state(self, state: dict) -> dict:
        """将一个实时的任务状态对象转换为一个 JSON 可序列化的字典，用于任务恢复。"""
        extractor = state['extractor']
        if not extractor: return None
        return {"infohash": state['infohash'],"piece_length": state['piece_length'],"video_file_offset": state['video_file_offset'],"video_file_size": state['video_file_size'],"extractor_info": {"extradata": extractor.extradata,"codec_name": extractor.codec_name,"mode": extractor.mode,"nal_length_size": extractor.nal_length_size,"timescale": extractor.timescale,"samples": [s._asdict() for s in extractor.samples],},"all_keyframes": [k._asdict() for k in state['all_keyframes']],"selected_keyframes": [k._asdict() for k in state['selected_keyframes']],"completed_pieces": list(state['completed_pieces']),"processed_keyframes": list(state['processed_keyframes']),}

    def _load_state_from_resume_data(self, data: dict) -> dict:
        """从 resume_data 字典中恢复任务状态。"""
        extractor = KeyframeExtractor(moov_data=None)
        ext_info = data['extractor_info']
        extractor.extradata = ext_info['extradata']
        extractor.codec_name = ext_info.get('codec_name') # 使用 .get 兼容旧的 resume_data
        extractor.mode = ext_info['mode']
        extractor.nal_length_size = ext_info['nal_length_size']
        extractor.timescale = ext_info['timescale']
        extractor.samples = [SampleInfo(**s) for s in ext_info['samples']]
        all_keyframes = [Keyframe(**k) for k in data['all_keyframes']]
        extractor.keyframes = all_keyframes
        return {"infohash": data['infohash'],"piece_length": data['piece_length'],"video_file_offset": data['video_file_offset'],"video_file_size": data['video_file_size'],"extractor": extractor,"all_keyframes": all_keyframes,"selected_keyframes": [Keyframe(**k) for k in data['selected_keyframes']],"completed_pieces": set(data['completed_pieces']),"processed_keyframes": set(data['processed_keyframes']),}

    async def _process_keyframe_pieces(
        self,
        handle,
        local_queue: asyncio.Queue,
        task_state: dict,
        keyframe_info: dict,
        piece_to_keyframes: dict,
        remaining_keyframes: list
    ) -> Tuple[set, dict]:
        """
        处理关键帧数据块的下载和截图任务的生成。
        返回一个元组 (本次运行中已处理的关键帧索引集合, {关键帧索引: 截图任务})。
        """
        infohash_hex = task_state['infohash']
        extractor = task_state['extractor']
        video_file_offset = task_state['video_file_offset']
        piece_length = task_state['piece_length']

        processed_this_run = set()
        generation_tasks_map = {}

        async def process_and_generate_task(keyframe_index):
            """辅助函数，用于处理单个关键帧的数据并创建生成任务。"""
            info = keyframe_info.get(keyframe_index)
            if not info: return None
            keyframe = info['keyframe']
            sample = extractor.samples[keyframe.sample_index - 1]
            try:
                keyframe_pieces_data = await self.client.fetch_pieces(
                    handle, self._get_pieces_for_range(video_file_offset + sample.offset, sample.size, piece_length),
                    timeout=self.settings.piece_fetch_timeout
                )
                packet_data_bytes = self._assemble_data_from_pieces(
                    keyframe_pieces_data, video_file_offset + sample.offset, sample.size, piece_length
                )
            except TorrentClientError as e:
                self.log.warning(f"获取关键帧 {keyframe.index} 数据失败: {e}，跳过。")
                return None
            if not packet_data_bytes or len(packet_data_bytes) != sample.size:
                self.log.warning(f"关键帧 {keyframe.index} 的数据不完整 ({len(packet_data_bytes)}/{sample.size})，跳过。")
                return None

            if extractor.mode == 'avc1':
                annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
                while cursor < len(packet_data_bytes):
                    nal_length = int.from_bytes(packet_data_bytes[cursor: cursor + extractor.nal_length_size], 'big')
                    cursor += extractor.nal_length_size
                    nal_data = packet_data_bytes[cursor: cursor + nal_length]
                    annexb_data.extend(start_code + nal_data)
                    cursor += nal_length
                packet_data = bytes(annexb_data)
            else:
                packet_data = packet_data_bytes

            ts_sec = keyframe.pts / keyframe.timescale if keyframe.timescale > 0 else keyframe.index
            m, s = divmod(ts_sec, 60); h, m = divmod(m, 60)
            timestamp_str = f"{int(h):02d}-{int(m):02d}-{int(s):02d}"
            return self.loop.create_task(self.generator.generate(
                codec_name=extractor.codec_name, extradata=extractor.extradata, packet_data=packet_data,
                infohash_hex=infohash_hex, timestamp_str=timestamp_str
            ))

        torrent_is_complete = False
        while len(processed_this_run) < len(remaining_keyframes):
            try:
                finished_piece = await asyncio.wait_for(local_queue.get(), timeout=self.settings.piece_queue_timeout)
                if finished_piece is None:
                    self.log.info(f"[{infohash_hex}] 收到 Torrent 完成信号。")
                    torrent_is_complete = True
                    break
            except asyncio.TimeoutError:
                self.log.warning(f"[{infohash_hex}] 等待 piece 超时。")
                break
            task_state.setdefault('completed_pieces', set()).add(finished_piece)
            if finished_piece not in piece_to_keyframes: continue
            for kf_index in piece_to_keyframes[finished_piece]:
                if kf_index in processed_this_run: continue
                info = keyframe_info.get(kf_index)
                if not info: continue
                info['needed_pieces'].remove(finished_piece)
                if not info['needed_pieces']:
                    self.log.info(f"[{infohash_hex}] 关键帧 {kf_index} 的所有 piece 都已准备好。正在生成截图任务。")
                    task = await process_and_generate_task(kf_index)
                    if task:
                        generation_tasks_map[kf_index] = task
                    processed_this_run.add(kf_index)

        if torrent_is_complete:
            final_try_keyframes = [kf for kf in remaining_keyframes if kf.index not in processed_this_run]
            if final_try_keyframes:
                self.log.info(f"[{infohash_hex}] Torrent 已完成，正在最后尝试处理 {len(final_try_keyframes)} 个剩余的关键帧。")
                for kf_index in [kf.index for kf in final_try_keyframes]:
                    task = await process_and_generate_task(kf_index)
                    if task:
                        generation_tasks_map[kf_index] = task
                    processed_this_run.add(kf_index)

        return processed_this_run, generation_tasks_map

    async def _generate_screenshots_from_torrent(self, handle, infohash_hex, resume_data=None):
        """处理为给定 torrent 生成截图的详细逻辑。"""
        task_state = {}
        if resume_data:
            self.log.info("[%s] 正在从提供的数据恢复任务。", infohash_hex)
            try:
                task_state = self._load_state_from_resume_data(resume_data)
                # 兼容性检查：如果旧的 resume_data 没有 codec_name，则任务无法继续
                if not task_state.get('extractor') or not task_state['extractor'].codec_name:
                    self.log.warning("[%s] 恢复数据缺少 codec_name 信息，将重新开始。", infohash_hex)
                    resume_data = None
            except (KeyError, TypeError) as e:
                self.log.error("[%s] 加载恢复数据失败，将重新开始。错误: %s", infohash_hex, e)
                resume_data = None

        if not resume_data:
            self.log.info("[%s] 正在开始新的截图任务分析。", infohash_hex)
            ti = handle.get_torrent_info()
            piece_length = ti.piece_length()
            video_file_index, video_file_size, video_file_offset = self._find_video_file(ti)
            if video_file_index == -1:
                raise NoVideoFileError("在 torrent 中没有找到 .mp4 文件。", infohash_hex)
            video_file_path = ti.files().file_path(video_file_index)
            self.log.info("[%s] 找到视频文件: '%s' (%d 字节)。", infohash_hex, video_file_path, video_file_size)
            moov_data = await self._get_moov_atom_data(handle, video_file_offset, video_file_size, piece_length, infohash_hex)
            try:
                extractor = KeyframeExtractor(moov_data)
                if not extractor.keyframes:
                    raise MoovParsingError("无法从 moov atom 中提取任何关键帧。", infohash_hex)
            except Exception as e:
                raise MoovParsingError(f"解析 moov 数据失败: {e}", infohash_hex) from e
            all_keyframes = extractor.keyframes
            selected_keyframes = self._select_keyframes(all_keyframes, extractor.timescale, extractor.samples)
            self.log.info("[%s] 从总共 %d 个关键帧中选择了 %d 个。", infohash_hex, len(all_keyframes), len(selected_keyframes))
            task_state = {"infohash": infohash_hex, "piece_length": piece_length,"video_file_offset": video_file_offset, "video_file_size": video_file_size,"extractor": extractor, "all_keyframes": all_keyframes,"selected_keyframes": selected_keyframes, "completed_pieces": set(),"processed_keyframes": set()}

        remaining_keyframes = [kf for kf in task_state['selected_keyframes'] if kf.index not in task_state.get('processed_keyframes', set())]
        self.log.info("[%s] 需要处理 %d 个关键帧。", infohash_hex, len(remaining_keyframes))
        if not remaining_keyframes:
            self.log.info("[%s] 所有选定的关键帧都已处理。", infohash_hex)
            return

        keyframe_info = {}
        piece_to_keyframes = defaultdict(list)
        all_needed_pieces = set()
        for kf in remaining_keyframes:
            sample = task_state['extractor'].samples[kf.sample_index - 1]
            offset = task_state['video_file_offset'] + sample.offset
            needed = self._get_pieces_for_range(offset, sample.size, task_state['piece_length'])
            keyframe_info[kf.index] = {'keyframe': kf, 'needed_pieces': set(needed)}
            for piece_idx in needed:
                piece_to_keyframes[piece_idx].append(kf.index)
            all_needed_pieces.update(needed)

        pieces_to_request = list(all_needed_pieces - task_state.get('completed_pieces', set()))
        self.log.info("[%s] 正在从 torrent 客户端请求 %d 个新 piece。", infohash_hex, len(pieces_to_request))

        local_queue = asyncio.Queue()
        self.client.subscribe_pieces(infohash_hex, local_queue)

        try:
            self.client.request_pieces(handle, pieces_to_request)
            processed_this_run, generation_tasks_map = await self._process_keyframe_pieces(
                handle, local_queue, task_state, keyframe_info, piece_to_keyframes, remaining_keyframes
            )

            if not generation_tasks_map and len(remaining_keyframes) > 0:
                 task_state.setdefault('processed_keyframes', set()).update(processed_this_run)
                 raise FrameDownloadTimeoutError("没有成功下载任何关键帧的数据。", infohash_hex, resume_data=self._serialize_task_state(task_state))

            # Important: We are gathering the tasks which are the *values* of the map.
            results = await asyncio.gather(*generation_tasks_map.values(), return_exceptions=True)
        finally:
            self.client.unsubscribe_pieces(infohash_hex, local_queue)

        successful_generations = 0
        # Create a list of keyframe indices from the map to iterate over, as the results correspond to this order.
        kf_indices = list(generation_tasks_map.keys())
        for i, result in enumerate(results):
            kf_index = kf_indices[i]
            if isinstance(result, Exception):
                self.log.error(f"生成截图时发生错误 (关键帧索引: {kf_index}): {result}", exc_info=result)
            else:
                successful_generations += 1

        task_state.setdefault('processed_keyframes', set()).update(generation_tasks_map.keys())

        if successful_generations < len(remaining_keyframes):
            # 如果部分成功，也认为是可恢复的失败，以便可以重试失败的部分
            self.log.warning("[%s] 未能为所有选定的关键帧生成截图 (%d/%d 成功)。", infohash_hex, successful_generations, len(remaining_keyframes))
            raise FrameDownloadTimeoutError(
                f"未能为所有选定的关键帧生成截图 ({successful_generations}/{len(remaining_keyframes)} 成功)。",
                infohash_hex,
                resume_data=self._serialize_task_state(task_state)
            )
        else:
            self.log.info("[%s] 截图任务成功完成。", infohash_hex)

    async def _send_status_update(self, **kwargs: Any) -> None:
        """Helper to send a status update via the callback, if provided."""
        if self.status_callback:
            # We await the callback directly to ensure it completes,
            # which is especially important in a testing context.
            await self.status_callback(**kwargs)

    async def _handle_screenshot_task(self, task_info: dict):
        infohash_hex = task_info['infohash']
        resume_data = task_info.get('resume_data')
        metadata = task_info.get('metadata')

        log_message = "正在处理任务: %s"
        log_args = [infohash_hex]
        if resume_data:
            log_message += " (正在恢复)"
        self.log.info(log_message, *log_args)

        try:
            async with self.client.get_handle(infohash_hex, metadata=metadata) as handle:
                await self._generate_screenshots_from_torrent(handle, infohash_hex, resume_data)

            # 如果没有异常抛出，则任务成功
            self.log.info("任务 %s 成功完成。", infohash_hex)
            await self._send_status_update(status='success', infohash=infohash_hex, message='任务成功完成。')

        except (FrameDownloadTimeoutError, MetadataTimeoutError, MoovFetchError, asyncio.TimeoutError) as e:
            self.log.warning("任务 %s 因可恢复的错误而失败: %s", infohash_hex, e)
            infohash = getattr(e, 'infohash', infohash_hex)
            await self._send_status_update(status='recoverable_failure',infohash=infohash,message=str(e),error=e,resume_data=getattr(e, 'resume_data', None))
        except TaskError as e:
            self.log.error("任务 %s 因永久性错误而失败: %s", e.infohash, e)
            await self._send_status_update(status='permanent_failure',infohash=e.infohash,message=str(e),error=e)
        except TorrentClientError as e:
            self.log.error("任务 %s 因 torrent 客户端错误而失败: %s", infohash_hex, e)
            await self._send_status_update(status='permanent_failure',infohash=infohash_hex,message=str(e),error=e)
        except Exception as e:
            self.log.exception("处理 %s 时发生意外的严重错误。", infohash_hex)
            await self._send_status_update(status='permanent_failure',infohash=infohash_hex,message=f"发生意外错误: {e}",error=e)
        finally:
            # 无论成功或失败，最后都将任务从活跃集合中移除
            self.active_tasks.discard(infohash_hex)

    async def _worker(self):
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("截图工作进程中发生错误。")
