# -*- coding: utf-8 -*-
"""
本模块定义了 ScreenshotService，它是协调整个截图生成过程的核心服务。
"""
import asyncio
import logging
import io
import struct
import base64
from typing import Generator, Tuple, Optional, Callable, Awaitable, Any
from collections import defaultdict

import av

from .client import TorrentClient, TorrentClientError
from .errors import (
    TaskError, NoVideoFileError, MoovNotFoundError, MoovFetchError,
    MoovParsingError, MetadataTimeoutError, FrameDownloadTimeoutError, MP4ParsingError
)
from .extractor import KeyframeExtractor, Keyframe, SampleInfo
from .generator import ScreenshotGenerator
from config import Settings

# 为 status_callback 定义一个类型签名，以增强可读性和静态检查能力
StatusCallback = Callable[..., Awaitable[None]]

class ScreenshotService:
    """
    协调截图生成过程的核心服务类。

    此类通过内部的 asyncio.Queue 管理任务，并由多个工作协程并发处理。
    其主要职责包括：
    1.  与 TorrentClient 交互，以下载 Torrent 元数据和所需的数据块 (pieces)。
    2.  解析视频文件 (MP4)，智能定位 'moov' atom。
    3.  使用 KeyframeExtractor 从 'moov' atom 中提取关键帧信息。
    4.  根据策略选择一部分关键帧进行截图。
    5.  计算并请求这些关键帧所需的数据块。
    6.  将下载好的数据块传递给 ScreenshotGenerator 以生成图片。
    7.  管理任务状态，支持从失败中恢复 (断点续传)。
    8.  通过回调函数向上层报告任务的最终状态和生成的截图。
    """
    def __init__(self, settings: Settings, loop=None, client=None, status_callback: Optional[StatusCallback] = None, screenshot_callback: Optional[Callable] = None, details_callback: Optional[Callable] = None):
        self.loop = loop or asyncio.get_event_loop()
        self.settings = settings
        self.log = logging.getLogger("ScreenshotService")
        self.task_queue = asyncio.Queue()
        self.workers = []
        self._running = False

        self.client = client or TorrentClient(
            loop=self.loop,
            settings=self.settings
        )
        self.generator = ScreenshotGenerator(
            loop=self.loop,
            output_dir=self.settings.output_dir,
            on_success=screenshot_callback
        )
        self.status_callback = status_callback
        self.details_callback = details_callback
        self.active_tasks = set()
        self._submit_lock = asyncio.Lock()

    def get_queue_size(self) -> int:
        """返回当前在服务内部队列中等待的任务数量。"""
        return self.task_queue.qsize()

    async def run(self):
        """启动服务，包括底层的 Torrent 客户端和处理任务的工作协程。"""
        self.log.info("正在启动 ScreenshotService...")
        self._running = True
        await self.client.start()
        for _ in range(self.settings.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info("ScreenshotService 已启动，拥有 %d 个工作进程。", self.settings.num_workers)

    async def stop(self):
        """异步地、优雅地停止服务，包括 Torrent 客户端和所有工作协程。"""
        self.log.info("正在停止 ScreenshotService...")
        self._running = False
        await self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("ScreenshotService 已停止。")

    async def submit_task(self, infohash: str, metadata: bytes = None, resume_data: dict = None):
        """
        提交一个新的截图任务。
        使用锁来防止同一 infohash 的任务被重复提交。
        """
        async with self._submit_lock:
            if infohash in self.active_tasks:
                self.log.warning("任务 %s 已在处理中，本次提交被忽略。", infohash)
                return
            self.active_tasks.add(infohash)

        await self.task_queue.put({'infohash': infohash, 'metadata': metadata, 'resume_data': resume_data})
        log_msg = "为 infohash: %s 重新提交了任务" if resume_data else "为 infohash: %s 提交了新任务"
        self.log.info(log_msg, infohash)

    def _get_pieces_for_range(self, offset_in_torrent: int, size: int, piece_length: int) -> list[int]:
        """为 torrent 中的给定字节范围计算其覆盖的所有 piece 索引。"""
        if size <= 0: return []
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length
        return list(range(start_piece, end_piece + 1))

    def _assemble_data_from_pieces(self, pieces_data: dict[int, bytes], offset_in_torrent: int, size: int, piece_length: int) -> bytes:
        """
        从多个 piece 数据块中，根据偏移量和大小，精确地拼接出所需的数据段。
        在拼接前会检查所有需要的 piece 是否都已存在。

        :param pieces_data: 一个字典，键是 piece 索引，值是该 piece 的 bytes 数据。
        :param offset_in_torrent: 所需数据段在整个 torrent 文件中的起始偏移量。
        :param size: 所需数据段的大小。
        :param piece_length: torrent 的标准 piece 大小。
        :return: 拼接好的 bytes 数据，如果缺少任何一个必需的 piece，则返回空 bytes。
        """
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length

        # 1. 前置检查：确保所有需要的 piece 都已下载。
        for piece_index in range(start_piece, end_piece + 1):
            if piece_index not in pieces_data:
                self.log.warning("组装数据时缺少 piece #%d，操作中止。", piece_index)
                return b""

        # 2. 核心逻辑：遍历所有相关的 piece，并从中拷贝出需要的部分。
        buffer = bytearray(size)
        buffer_offset = 0
        for piece_index in range(start_piece, end_piece + 1):
            piece_data = pieces_data[piece_index]

            # 计算在此 piece 内需要拷贝的起始和结束位置
            # 对于起始 piece，需要从特定偏移量开始拷贝
            copy_from_start = offset_in_torrent % piece_length if piece_index == start_piece else 0
            # 对于结束 piece，只拷贝到所需数据的末尾
            copy_to_end = (offset_in_torrent + size - 1) % piece_length + 1 if piece_index == end_piece else piece_length

            # 从当前 piece 中提取出相关的部分
            chunk = piece_data[copy_from_start:copy_to_end]

            # 计算实际要拷贝到最终 buffer 的字节数，防止越界
            bytes_to_copy = min(len(chunk), size - buffer_offset)
            if bytes_to_copy > 0:
                buffer[buffer_offset : buffer_offset + bytes_to_copy] = chunk[:bytes_to_copy]
                buffer_offset += bytes_to_copy
        return bytes(buffer)

    def _parse_mp4_boxes(self, stream: io.BytesIO) -> Generator[Tuple[str, bytes, int, int], None, None]:
        """
        一个更健壮的 MP4 box 解析器。
        如果一个 box 声明的大小超出了可用数据范围，它不会失败，而是会 yield
        它所拥有的部分数据，同时仍然报告在头部声明的完整大小。
        这允许调用者决定如何处理部分 box。
        """
        stream_buffer = stream.getbuffer()
        buffer_size = len(stream_buffer)
        current_offset = stream.tell()
        while current_offset <= buffer_size - 8:
            stream.seek(current_offset)
            try:
                header_data = stream.read(8)
                if len(header_data) < 8: break
                declared_size, box_type_bytes = struct.unpack('>I4s', header_data)
                box_type = box_type_bytes.decode('ascii', 'ignore')
            except struct.error:
                self.log.warning("在偏移量 %d 处解析 MP4 box 头部时遇到 struct.error。", current_offset)
                break

            box_header_size = 8
            if declared_size == 1: # 64-bit size
                if current_offset + 16 > buffer_size:
                    self.log.warning("Box '%s' 在偏移量 %d 处需要 64 位大小，但数据不足。", box_type, current_offset)
                    break
                declared_size = struct.unpack('>Q', stream.read(8))[0]
                box_header_size = 16
            elif declared_size == 0: # Extends to end of file
                declared_size = buffer_size - current_offset

            if declared_size < box_header_size:
                self.log.warning("Box '%s' 在偏移量 %d 处声明了无效的大小 %d。", box_type, current_offset, declared_size)
                break

            # 关键改动：不再因为 box 超出范围而抛出异常。
            # 我们计算在此缓冲区中实际可用的 box 大小。
            effective_box_size = declared_size
            if current_offset + declared_size > buffer_size:
                self.log.debug("Box '%s' (大小: %d) 超出了缓冲区大小 %d。将 yield 部分数据。", box_type, declared_size, buffer_size)
                effective_box_size = buffer_size - current_offset

            # 我们 yield 我们拥有的数据（可能是部分的），但同时报告在头部声明的 *完整* 大小。
            box_content = stream_buffer[current_offset : current_offset + effective_box_size]
            yield box_type, bytes(box_content), current_offset, declared_size

            current_offset += declared_size

    async def _get_moov_atom_data(self, handle, video_file_offset, video_file_size, piece_length, infohash_hex) -> bytes:
        """
        智能地查找并获取 'moov' atom 数据。
        策略是：首先探测文件头部，如果找不到 'moov'，则探测文件尾部。
        这是因为 'moov' atom 可能位于文件的开头或结尾。
        """
        # --- 阶段1: 探测文件头部 ---
        try:
            head_size = min(256 * 1024, video_file_size)
            if head_size > 0:
                head_pieces = self._get_pieces_for_range(video_file_offset, head_size, piece_length)
                head_data_pieces = await self.client.fetch_pieces(handle, head_pieces, timeout=self.settings.moov_probe_timeout)
                head_data = self._assemble_data_from_pieces(head_data_pieces, video_file_offset, head_size, piece_length)
                stream = io.BytesIO(head_data)

                # 使用新的、更健壮的解析器
                for box_type, partial_box_data, box_offset, declared_size in self._parse_mp4_boxes(stream):
                    if box_type == 'moov':
                        # 检查我们拥有的数据是否就是完整的 box
                        if len(partial_box_data) >= declared_size:
                            self.log.info("[%s] 在头部探测中找到了完整的 'moov' box。", infohash_hex)
                            return partial_box_data

                        # 如果数据不完整，说明 'moov' box 太大，需要专门下载它
                        self.log.info("[%s] 在头部探测中找到了一个大小为 %d 的部分 'moov' box。现在将获取完整的 box。", infohash_hex, declared_size)
                        full_moov_offset_in_torrent = video_file_offset + box_offset
                        needed_pieces = self._get_pieces_for_range(full_moov_offset_in_torrent, declared_size, piece_length)
                        moov_data_pieces = await self.client.fetch_pieces(handle, needed_pieces, timeout=self.settings.moov_probe_timeout)
                        return self._assemble_data_from_pieces(moov_data_pieces, full_moov_offset_in_torrent, declared_size, piece_length)

                    if box_type == 'mdat':
                        # 如果先找到 'mdat'，说明 'moov' 很可能在尾部，停止头部探测
                        self.log.info("[%s] 在文件头部探测到 'mdat'，将转而探测文件尾部。", infohash_hex)
                        break
        except TorrentClientError as e:
            # 捕获 Torrent 客户端本身的错误（例如，超时）
            raise MoovFetchError(f"在 moov 头部探测期间获取 piece 失败: {e}", infohash_hex) from e

        # --- 阶段2: 探测文件尾部 ---
        try:
            tail_probe_size = 10 * 1024 * 1024
            tail_file_offset = max(0, video_file_size - tail_probe_size)
            tail_torrent_offset = video_file_offset + tail_file_offset
            tail_size = min(tail_probe_size, video_file_size - tail_file_offset)
            if tail_size > 0:
                tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)
                tail_data_pieces = await self.client.fetch_pieces(handle, tail_pieces, timeout=self.settings.moov_probe_timeout)
                tail_data = self._assemble_data_from_pieces(tail_data_pieces, tail_torrent_offset, tail_size, piece_length)

                # 从后向前搜索 'moov' 标志，然后验证其是否为一个有效的 box
                search_pos = len(tail_data)
                while (found_pos := tail_data.rfind(b'moov', 0, search_pos)) != -1:
                    potential_start_pos = found_pos - 4
                    if potential_start_pos < 0: search_pos = found_pos; continue
                    stream = io.BytesIO(tail_data); stream.seek(potential_start_pos)
                    try:
                        box_size, box_type_bytes = struct.unpack('>I4s', stream.read(8))
                        if box_type_bytes == b'moov' and box_size > 8 and (potential_start_pos + box_size <= len(tail_data)):
                            stream.seek(potential_start_pos)
                            _, full_box_data, _, parsed_box_size = next(self._parse_mp4_boxes(stream), (None, None, None, None))
                            if full_box_data: return full_box_data
                    except (struct.error, MP4ParsingError): pass
                    search_pos = found_pos
        except TorrentClientError as e:
            raise MoovFetchError(f"在 moov 尾部探测期间失败: {e}", infohash_hex) from e

        raise MoovNotFoundError("无法在文件的头部或尾部定位 'moov' atom。", infohash_hex)

    def _find_video_file(self, ti: "lt.torrent_info") -> Tuple[int, int, int, Optional[str]]:
        """在 torrent 中查找最大的视频文件（目前仅支持.mp4）并返回其信息。"""
        video_file_index, video_file_size, video_file_offset, video_filename = -1, -1, -1, None
        fs = ti.files()
        for i in range(fs.num_files()):
            file_path = fs.file_path(i)
            if file_path.lower().endswith('.mp4') and fs.file_size(i) > video_file_size:
                video_file_size = fs.file_size(i)
                video_file_index = i
                video_file_offset = fs.file_offset(i)
                video_filename = file_path
        return video_file_index, video_file_size, video_file_offset, video_filename

    def _select_keyframes(self, all_keyframes: list[Keyframe], timescale: int, duration_pts: int, samples: list = None) -> list[Keyframe]:
        """
        根据关键帧的显示时间戳 (PTS) 从所有关键帧中均匀地选择一个代表性子集。
        这种方法确保了截图在视频的时间线上是均匀分布的，而不是基于关键帧在列表中的索引。

        :param all_keyframes: 包含所有关键帧的列表。
        :param timescale: 视频的 timescale，用于将 PTS 转换为秒。
        :param duration_pts: 视频的总时长 (以 PTS 为单位)。
        :param samples: 视频的样本列表，用于在 duration_pts 不可用时作为备用。
        :return: 一个代表性的关键帧子集。
        """
        if not all_keyframes:
            return []

        # 如果从 'mdhd' box 中未能成功解析出时长，则回退到基于最后一个样本时间戳的估算
        if duration_pts == 0 and samples:
            self.log.warning("duration_pts 为 0，将使用最后一个样本的 PTS 作为估算时长。")
            duration_pts = samples[-1].pts

        duration_sec = duration_pts / timescale if timescale > 0 else 0

        # 根据视频时长和配置计算目标截图数量
        num_screenshots = self.settings.default_screenshots
        if duration_sec > 0:
            num_screenshots = max(
                self.settings.min_screenshots,
                min(int(duration_sec / self.settings.target_interval_sec), self.settings.max_screenshots)
            )

        if len(all_keyframes) <= num_screenshots:
            return all_keyframes

        # 计算目标时间点
        target_timestamps_pts = [
            int(i * duration_pts / num_screenshots) for i in range(num_screenshots)
        ]

        selected_keyframes = []
        # 对于每个目标时间点，找到 PTS 最接近它的关键帧
        for target_pts in target_timestamps_pts:
            # 使用 min 函数和一个 lambda 来找到差值最小的关键帧
            closest_keyframe = min(
                all_keyframes,
                key=lambda kf: abs(kf.pts - target_pts)
            )
            if closest_keyframe not in selected_keyframes:
                selected_keyframes.append(closest_keyframe)

        # 按 PTS 排序，以确保截图顺序与视频播放顺序一致
        selected_keyframes.sort(key=lambda kf: kf.pts)
        return selected_keyframes

    def _serialize_task_state(self, state: dict) -> dict:
        """将一个实时的、包含复杂对象的任务状态，转换为一个 JSON 可序列化的字典，用于任务恢复。"""
        extractor = state.get('extractor')
        if not extractor: return None
        # 注意：extradata 是 bytes 类型，在发送给调度器前需要由 worker 进行 base64 编码
        return {
            "infohash": state['infohash'], "piece_length": state['piece_length'],
            "video_file_offset": state['video_file_offset'], "video_file_size": state['video_file_size'],
            "extractor_info": {
                "extradata": extractor.extradata, "codec_name": extractor.codec_name, "mode": extractor.mode,
                "nal_length_size": extractor.nal_length_size, "timescale": extractor.timescale,
                "samples": [s._asdict() for s in extractor.samples],
            },
            "all_keyframes": [k._asdict() for k in state['all_keyframes']],
            "selected_keyframes": [k._asdict() for k in state['selected_keyframes']],
            "completed_pieces": list(state.get('completed_pieces', [])),
            "processed_keyframes": list(state.get('processed_keyframes', [])),
        }

    def _load_state_from_resume_data(self, data: dict) -> dict:
        """从 resume_data 字典中恢复任务状态，将纯数据结构重建为包含类的实例的对象。"""
        extractor = KeyframeExtractor(moov_data=None)
        ext_info = data['extractor_info']

        # extradata 在 resume_data 中是 base64 编码的字符串，需要解码回 bytes
        extradata = ext_info.get('extradata')
        if extradata and isinstance(extradata, str):
            try: extractor.extradata = base64.b64decode(extradata)
            except (base64.binascii.Error, TypeError) as e:
                self.log.error(f"解码 base64 extradata 失败: {e}"); extractor.extradata = None
        else: extractor.extradata = extradata

        extractor.codec_name = ext_info.get('codec_name')
        extractor.mode = ext_info['mode']
        extractor.nal_length_size = ext_info['nal_length_size']
        extractor.timescale = ext_info['timescale']
        extractor.samples = [SampleInfo(**s) for s in ext_info['samples']]
        all_keyframes = [Keyframe(**k) for k in data['all_keyframes']]
        extractor.keyframes = all_keyframes
        return {
            "infohash": data['infohash'], "piece_length": data['piece_length'],
            "video_file_offset": data['video_file_offset'], "video_file_size": data['video_file_size'],
            "extractor": extractor, "all_keyframes": all_keyframes,
            "selected_keyframes": [Keyframe(**k) for k in data['selected_keyframes']],
            "completed_pieces": set(data['completed_pieces']),
            "processed_keyframes": set(data['processed_keyframes']),
        }

    async def _process_keyframe_pieces(self, handle, local_queue, task_state, keyframe_info, piece_to_keyframes, remaining_keyframes) -> Tuple[set, dict]:
        """
        监听已完成的 piece，当一个关键帧所需的所有 piece 都下载完毕时，为其创建截图生成任务。
        返回本次运行中成功生成截图任务的关键帧索引集合，以及 {关键帧索引: 截图任务} 的映射。
        """
        infohash_hex, extractor = task_state['infohash'], task_state['extractor']
        video_file_offset, piece_length = task_state['video_file_offset'], task_state['piece_length']
        processed_this_run, generation_tasks_map = set(), {}

        async def process_and_generate_task(keyframe_index):
            info = keyframe_info.get(keyframe_index)
            if not info: return None
            keyframe = info['keyframe']; sample = extractor.samples[keyframe.sample_index - 1]
            try:
                keyframe_pieces_data = await self.client.fetch_pieces(handle, self._get_pieces_for_range(video_file_offset + sample.offset, sample.size, piece_length), timeout=self.settings.piece_fetch_timeout)
                packet_data_bytes = self._assemble_data_from_pieces(keyframe_pieces_data, video_file_offset + sample.offset, sample.size, piece_length)
            except TorrentClientError as e:
                self.log.warning(f"获取关键帧 {keyframe.index} 数据失败: {e}，跳过。"); return None
            if not packet_data_bytes or len(packet_data_bytes) != sample.size:
                self.log.warning(f"关键帧 {keyframe.index} 的数据不完整 ({len(packet_data_bytes)}/{sample.size})，跳过。"); return None

            # 为 H.264/HEVC 的 'avc1' 格式（NALU长度前缀）转换为 Annex B 格式（起始码）
            if extractor.mode == 'avc1':
                annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
                while cursor < len(packet_data_bytes):
                    nal_length = int.from_bytes(packet_data_bytes[cursor : cursor + extractor.nal_length_size], 'big'); cursor += extractor.nal_length_size
                    nal_data = packet_data_bytes[cursor : cursor + nal_length]; annexb_data.extend(start_code + nal_data); cursor += nal_length
                packet_data = bytes(annexb_data)
            else: packet_data = packet_data_bytes

            ts_sec = keyframe.pts / keyframe.timescale if keyframe.timescale > 0 else keyframe.index
            timestamp_str = str(int(ts_sec))
            return self.loop.create_task(self.generator.generate(extractor.codec_name, extractor.extradata, packet_data, infohash_hex, timestamp_str))

        torrent_is_complete = False
        while len(processed_this_run) < len(remaining_keyframes):
            try:
                finished_piece = await asyncio.wait_for(local_queue.get(), timeout=self.settings.piece_queue_timeout)
                if finished_piece is None: torrent_is_complete = True; break
            except asyncio.TimeoutError: self.log.warning(f"[{infohash_hex}] 等待 piece 超时。"); break

            task_state.setdefault('completed_pieces', set()).add(finished_piece)
            if finished_piece not in piece_to_keyframes: continue
            for kf_index in piece_to_keyframes[finished_piece]:
                if kf_index in processed_this_run: continue
                info = keyframe_info.get(kf_index);
                if not info: continue
                info['needed_pieces'].remove(finished_piece)
                if not info['needed_pieces']:
                    task = await process_and_generate_task(kf_index)
                    if task: generation_tasks_map[kf_index] = task
                    processed_this_run.add(kf_index)

        # 如果整个 torrent 都下载完了，最后尝试一次处理所有剩余的关键帧
        if torrent_is_complete:
            final_try_keyframes = [kf for kf in remaining_keyframes if kf.index not in processed_this_run]
            for kf_index in [kf.index for kf in final_try_keyframes]:
                task = await process_and_generate_task(kf_index)
                if task: generation_tasks_map[kf_index] = task
                processed_this_run.add(kf_index)

        return processed_this_run, generation_tasks_map

    async def _generate_screenshots_from_torrent(self, handle, infohash_hex, resume_data=None):
        """处理为给定 torrent 生成截图的完整、复杂的业务逻辑。"""
        # --- 阶段 1: 恢复任务或开始新任务 ---
        task_state = {}
        if resume_data:
            try: task_state = self._load_state_from_resume_data(resume_data)
            except (KeyError, TypeError) as e: self.log.error("[%s] 加载恢复数据失败: %s", infohash_hex, e); resume_data = None

        if not resume_data:
            ti = handle.get_torrent_info()
            piece_length = ti.piece_length()
            video_file_index, video_file_size, video_file_offset, video_filename = self._find_video_file(ti)
            if video_file_index == -1: raise NoVideoFileError("在 torrent 中没有找到 .mp4 文件。", infohash_hex)
            moov_data = await self._get_moov_atom_data(handle, video_file_offset, video_file_size, piece_length, infohash_hex)
            try:
                extractor = KeyframeExtractor(moov_data)
                if not extractor.keyframes: raise MoovParsingError("无法从 moov atom 中提取任何关键帧。", infohash_hex)
            except (MP4ParsingError, Exception) as e: raise MoovParsingError(f"解析 moov 数据时失败: {e}", infohash_hex) from e

            if self.details_callback:
                duration_sec = extractor.duration_pts / extractor.timescale if extractor.timescale > 0 else 0
                details = {
                    "torrent_name": ti.name(),
                    "video_filename": video_filename,
                    "video_duration_seconds": int(duration_sec)
                }
                await self.details_callback(infohash_hex, details)

            all_keyframes = extractor.keyframes
            selected_keyframes = self._select_keyframes(
                all_keyframes, extractor.timescale, extractor.duration_pts, extractor.samples
            )
            task_state = {
                "infohash": infohash_hex, "piece_length": piece_length, "video_file_offset": video_file_offset,
                "video_file_size": video_file_size, "extractor": extractor, "all_keyframes": all_keyframes,
                "selected_keyframes": selected_keyframes, "completed_pieces": set(), "processed_keyframes": set()
            }

        # --- 阶段 2: 计算并请求所需的 piece ---
        remaining_keyframes = [kf for kf in task_state['selected_keyframes'] if kf.index not in task_state.get('processed_keyframes', set())]
        if not remaining_keyframes: self.log.info("[%s] 所有选定的关键帧都已处理。", infohash_hex); return

        keyframe_info, piece_to_keyframes, all_needed_pieces = {}, defaultdict(list), set()
        for kf in remaining_keyframes:
            sample = task_state['extractor'].samples[kf.sample_index - 1]
            offset = task_state['video_file_offset'] + sample.offset
            needed = self._get_pieces_for_range(offset, sample.size, task_state['piece_length'])
            keyframe_info[kf.index] = {'keyframe': kf, 'needed_pieces': set(needed)}
            for piece_idx in needed: piece_to_keyframes[piece_idx].append(kf.index)
            all_needed_pieces.update(needed)

        pieces_to_request = list(all_needed_pieces - task_state.get('completed_pieces', set()))
        local_queue = asyncio.Queue()
        self.client.subscribe_pieces(infohash_hex, local_queue)

        # --- 阶段 3: 处理 piece 并生成截图 ---
        try:
            self.client.request_pieces(handle, pieces_to_request)
            processed_this_run, generation_tasks_map = await self._process_keyframe_pieces(handle, local_queue, task_state, keyframe_info, piece_to_keyframes, remaining_keyframes)
            if not generation_tasks_map and remaining_keyframes:
                 task_state.setdefault('processed_keyframes', set()).update(processed_this_run)
                 raise FrameDownloadTimeoutError("没有成功下载任何关键帧的数据。", infohash_hex, resume_data=self._serialize_task_state(task_state))
            results = await asyncio.gather(*generation_tasks_map.values(), return_exceptions=True)
        finally:
            self.client.unsubscribe_pieces(infohash_hex, local_queue)

        # --- 阶段 4: 收集结果并判断最终状态 ---
        successful_kf_indices = set()
        kf_indices = list(generation_tasks_map.keys())
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.log.error(f"生成截图时发生错误 (关键帧索引: {kf_indices[i]}): {result}", exc_info=result)
            else: successful_kf_indices.add(kf_indices[i])

        task_state.setdefault('processed_keyframes', set()).update(successful_kf_indices)

        if len(successful_kf_indices) < len(remaining_keyframes):
            # 如果部分成功，也认为是可恢复的失败，以便可以重试失败的部分
            raise FrameDownloadTimeoutError(
                f"未能为所有选定的关键帧生成截图 ({len(successful_kf_indices)}/{len(remaining_keyframes)} 成功)。",
                infohash_hex, resume_data=self._serialize_task_state(task_state)
            )
        self.log.info("[%s] 截图任务成功完成。", infohash_hex)

    async def _send_status_update(self, **kwargs: Any) -> None:
        """一个辅助函数，用于安全地调用状态更新回调。"""
        if self.status_callback:
            await self.status_callback(**kwargs)

    async def _handle_screenshot_task(self, task_info: dict):
        """处理单个截图任务的完整生命周期，包括错误处理和状态报告。"""
        infohash_hex = task_info['infohash']
        log_message = "正在处理任务: %s" + (" (正在恢复)" if task_info.get('resume_data') else "")
        self.log.info(log_message, infohash_hex)

        try:
            async with self.client.get_handle(infohash_hex, metadata=task_info.get('metadata')) as handle:
                await self._generate_screenshots_from_torrent(handle, infohash_hex, task_info.get('resume_data'))
            self.log.info("任务 %s 成功完成。", infohash_hex)
            await self._send_status_update(status='success', infohash=infohash_hex, message='任务成功完成。')
        except (FrameDownloadTimeoutError, MetadataTimeoutError, MoovFetchError, asyncio.TimeoutError) as e:
            self.log.warning("任务 %s 因可恢复的错误而失败: %s", infohash_hex, e)
            await self._send_status_update(status='recoverable_failure', infohash=getattr(e, 'infohash', infohash_hex), message=str(e), error=e, resume_data=getattr(e, 'resume_data', None))
            return
        except TaskError as e:
            self.log.error("任务 %s 因永久性错误而失败: %s", e.infohash, e, exc_info=True)
            await self._send_status_update(status='permanent_failure', infohash=e.infohash, message=str(e), error=e)
            return
        except Exception as e:
            self.log.exception("处理 %s 时发生意外的严重错误。", infohash_hex)
            await self._send_status_update(status='permanent_failure', infohash=infohash_hex, message=f"发生意外错误: {e}", error=e)
            return
        finally:
            self.active_tasks.discard(infohash_hex)

    async def _worker(self):
        """一个工作协程，不断地从任务队列中获取并处理任务。"""
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("截图工作进程中发生未捕获的错误。")
