# -*- coding: utf-8 -*-
"""
本模块提供了 ScreenshotService，它是协调整个截图生成过程的核心业务逻辑。
重构后的版本将巨大的 `_generate_screenshots_from_torrent` 方法分解为多个
更小、职责更单一的函数，并引入了一个状态管理类 `_TaskState` 来清晰地管理任务状态。
"""
import asyncio
import logging
import io
import struct
import base64
from dataclasses import dataclass, field
from typing import Generator, Tuple, Optional, Dict, Any, Callable, List, Set

from .client import TorrentClient, LibtorrentError
from .extractor import H264KeyframeExtractor, Keyframe
from .generator import ScreenshotGenerator

# --- 结果数据类 ---
@dataclass
class FatalErrorResult:
    infohash: str
    reason: str

@dataclass
class AllSuccessResult:
    infohash: str
    screenshots_count: int

@dataclass
class PartialSuccessResult:
    infohash: str
    screenshots_count: int
    reason: str
    resume_data: Dict[str, Any] = field(default_factory=dict)

# --- 内部状态管理 ---
@dataclass
class _TaskState:
    """封装单个截图任务的所有状态。"""
    infohash_hex: str
    handle: Any  # lt.torrent_handle
    extractor: H264KeyframeExtractor
    video_file_offset: int
    keyframes_to_process: List[Keyframe]
    all_kf_indices: List[int]
    processed_kf_indices: Set[int] = field(default_factory=set)
    screenshots_generated_so_far: int = 0

class ScreenshotService:
    def __init__(self, loop=None, num_workers=10, output_dir='./screenshots_output', torrent_save_path='/dev/shm', client_lru_cache_size=50):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotService")
        self.task_queue = asyncio.Queue()
        self.workers = []
        self._running = False
        self.client = TorrentClient(loop=self.loop, save_path=torrent_save_path, max_cache_size=client_lru_cache_size)
        self.generator = ScreenshotGenerator(loop=self.loop, output_dir=self.output_dir)

    async def run(self):
        self.log.info("正在启动 ScreenshotService...")
        self._running = True
        await self.client.start()
        for i in range(self.num_workers):
            worker = self.loop.create_task(self._worker())
            self.workers.append(worker)
        self.log.info(f"ScreenshotService 已启动，拥有 {self.num_workers} 个工作线程。")

    def stop(self):
        self.log.info("正在停止 ScreenshotService...")
        self._running = False
        self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("ScreenshotService 已停止。")

    async def submit_task(self, infohash: str, resume_data: Optional[Dict[str, Any]] = None, on_complete: Optional[Callable] = None):
        await self.task_queue.put({
            'infohash': infohash, 'resume_data': resume_data, 'on_complete': on_complete
        })
        self.log.info(f"已为 infohash 提交新任务: {infohash}")

    # --- 核心业务逻辑 ---

    async def _handle_screenshot_task(self, task_info: dict):
        infohash, resume_data, on_complete = task_info['infohash'], task_info['resume_data'], task_info['on_complete']
        self.log.info(f"正在处理 infohash 的任务: {infohash}")
        handle, result = None, None
        try:
            handle = await self.client.add_torrent(infohash)
            if not handle or not handle.is_valid():
                result = FatalErrorResult(infohash=infohash, reason="未能获取有效的 torrent 句柄")
            else:
                result = await self._generate_screenshots_from_torrent(handle, infohash, resume_data)
        except Exception as e:
            self.log.exception(f"处理 {infohash} 时发生意外的、未被捕获的错误。")
            result = FatalErrorResult(infohash=infohash, reason=f"意外的工作线程错误: {str(e)}")
        finally:
            if on_complete:
                try:
                    await self._execute_callback(on_complete, result)
                except Exception:
                    self.log.exception(f"为 {infohash} 执行 on_complete 回调时出错")

    async def _generate_screenshots_from_torrent(self, handle, infohash_hex: str, resume_data: Optional[Dict[str, Any]] = None):
        """为给定的 torrent 生成截图的主流程。"""
        try:
            state = await self._initialize_task_state(handle, infohash_hex, resume_data)
            if not state:
                return FatalErrorResult(infohash=infohash_hex, reason="无法初始化任务状态，可能是因为找不到视频文件或元数据。")
        except (LibtorrentError, asyncio.TimeoutError) as e:
            return PartialSuccessResult(infohash=infohash_hex, screenshots_count=0, reason=f"获取 moov atom 超时或出错: {e}", resume_data={})
        except Exception as e:
            return FatalErrorResult(infohash=infohash_hex, reason=f"初始化任务失败: {e}")

        if not state.keyframes_to_process:
            self.log.info(f"没有剩余的关键帧需要为 {infohash_hex} 处理。")
            return AllSuccessResult(infohash=infohash_hex, screenshots_count=state.screenshots_generated_so_far)

        return await self._execute_download_and_generate(state)

    async def _get_torrent_info_async(self, handle):
        """异步获取 torrent 信息，以便于测试时模拟。"""
        return await self.loop.run_in_executor(None, handle.get_torrent_info)

    async def _initialize_task_state(self, handle, infohash_hex: str, resume_data: Optional[Dict[str, Any]]) -> Optional[_TaskState]:
        """根据是新任务还是恢复任务，创建并返回任务状态对象。"""
        ti = await self._get_torrent_info_async(handle)

        if resume_data and resume_data.get('moov_data_b64'):
            self.log.info(f"使用 resume_data 恢复任务 {infohash_hex}。")
            moov_data = base64.b64decode(resume_data['moov_data_b64'])
            extractor = H264KeyframeExtractor(moov_data)
            all_keyframes = extractor.keyframes
            all_kf_indices = resume_data['all_kf_indices']
            processed_indices = set(resume_data.get('processed_kf_indices', []))

            keyframes_to_process = [kf for kf in all_keyframes if kf.index in all_kf_indices and kf.index not in processed_indices]

            return _TaskState(
                infohash_hex=infohash_hex, handle=handle, extractor=extractor,
                video_file_offset=resume_data['video_file_offset'],
                keyframes_to_process=keyframes_to_process,
                all_kf_indices=all_kf_indices,
                processed_kf_indices=processed_indices,
                screenshots_generated_so_far=resume_data.get('screenshots_generated_so_far', 0)
            )
        else:
            self.log.info(f"为 {infohash_hex} 开始新任务。")
            video_file = self._find_largest_mp4_file(ti)
            if not video_file: return None

            moov_data = await self._get_moov_atom_data(handle, video_file['offset'], video_file['size'], ti.piece_length())
            if not moov_data: raise LibtorrentError("无法找到或解析 moov atom")

            extractor = H264KeyframeExtractor(moov_data)
            if not extractor.keyframes: raise ValueError("无法从 moov atom 中提取任何关键帧")

            keyframes_to_process = self._select_keyframes(extractor)

            return _TaskState(
                infohash_hex=infohash_hex, handle=handle, extractor=extractor,
                video_file_offset=video_file['offset'],
                keyframes_to_process=keyframes_to_process,
                all_kf_indices=[kf.index for kf in keyframes_to_process]
            )

    def _select_keyframes(self, extractor: H264KeyframeExtractor) -> List[Keyframe]:
        """根据预设规则从提取器中选择要截图的关键帧。"""
        MIN_SCREENSHOTS, MAX_SCREENSHOTS, TARGET_INTERVAL_SEC = 5, 50, 180

        duration_sec = extractor.samples[-1].pts / extractor.timescale if extractor.timescale > 0 and extractor.samples else 0
        num_screenshots = max(MIN_SCREENSHOTS, min(int(duration_sec / TARGET_INTERVAL_SEC), MAX_SCREENSHOTS)) if duration_sec > 0 else 20

        if len(extractor.keyframes) <= num_screenshots:
            return extractor.keyframes
        else:
            indices = [int(i * len(extractor.keyframes) / num_screenshots) for i in range(num_screenshots)]
            return [extractor.keyframes[i] for i in sorted(list(set(indices)))]

    async def _execute_download_and_generate(self, state: _TaskState):
        """执行 piece 下载、数据组装和截图生成的循环。"""
        piece_length = state.handle.get_torrent_info().piece_length()
        keyframe_info, piece_to_keyframes, all_needed_pieces = self._build_piece_download_plan(state)

        await self.client.request_pieces(state.handle, list(all_needed_pieces))

        newly_processed_indices, generation_tasks = set(), []
        timeout = getattr(self, 'TIMEOUT_FOR_TESTING', 300)

        try:
            while len(newly_processed_indices) < len(state.keyframes_to_process):
                finished_piece = await asyncio.wait_for(self.client.finished_piece_queue.get(), timeout=timeout)
                if finished_piece not in piece_to_keyframes: continue

                for kf_index in piece_to_keyframes.pop(finished_piece, []):
                    info = keyframe_info.get(kf_index)
                    if not info or kf_index in newly_processed_indices: continue
                    info['needed_pieces'].remove(finished_piece)

                    if not info['needed_pieces']:
                        self._check_simulated_failure(len(newly_processed_indices))
                        task = asyncio.create_task(self._process_and_generate_screenshot(
                            info['keyframe'], state.extractor, state.handle, state.video_file_offset, piece_length, state.infohash_hex
                        ))
                        generation_tasks.append(task)
                        newly_processed_indices.add(kf_index)
                self.client.finished_piece_queue.task_done()
        except asyncio.TimeoutError:
            return self._create_partial_success_result(state, generation_tasks, newly_processed_indices)

        return self._create_all_success_result(state, generation_tasks)

    def _build_piece_download_plan(self, state: _TaskState) -> Tuple[Dict, Dict, Set]:
        """为待处理的关键帧构建下载计划。"""
        keyframe_info, piece_to_keyframes, all_needed_pieces = {}, {}, set()
        for kf in state.keyframes_to_process:
            sample = state.extractor.samples[kf.sample_index - 1]
            needed = self._get_pieces_for_range(state.video_file_offset + sample.offset, sample.size, state.handle.get_torrent_info().piece_length())
            keyframe_info[kf.index] = {'keyframe': kf, 'needed_pieces': set(needed)}
            for piece_idx in needed:
                piece_to_keyframes.setdefault(piece_idx, set()).add(kf.index)
            all_needed_pieces.update(needed)
        return keyframe_info, piece_to_keyframes, all_needed_pieces

    # --- 结果创建辅助函数 ---

    async def _create_all_success_result(self, state: _TaskState, generation_tasks: List[asyncio.Task]) -> AllSuccessResult:
        screenshots_this_run = sum(1 for r in await asyncio.gather(*generation_tasks) if r is True)
        total_screenshots = state.screenshots_generated_so_far + screenshots_this_run
        self.log.info(f"{state.infohash_hex} 的截图任务完成。本次运行生成了 {screenshots_this_run} 张截图，总计 {total_screenshots} 张。")
        return AllSuccessResult(infohash=state.infohash_hex, screenshots_count=total_screenshots)

    def _create_partial_success_result(self, state: _TaskState, generation_tasks: List[asyncio.Task], newly_processed_indices: Set[int]) -> PartialSuccessResult:
        successful_tasks = [t for t in generation_tasks if t.done() and not t.cancelled() and t.exception() is None and t.result() is True]
        screenshots_this_run = len(successful_tasks)

        final_processed_indices = sorted(list(state.processed_kf_indices.union(newly_processed_indices)))
        total_screenshots_so_far = state.screenshots_generated_so_far + screenshots_this_run

        moov_data = state.extractor.moov_stream.getvalue()
        rich_resume_data = {
            "moov_data_b64": base64.b64encode(moov_data).decode('ascii'),
            "video_file_offset": state.video_file_offset,
            "all_kf_indices": state.all_kf_indices,
            "processed_kf_indices": final_processed_indices,
            "screenshots_generated_so_far": total_screenshots_so_far,
        }
        reason = f"等待 pieces 超时。处理了 {len(state.keyframes_to_process)} 帧中的 {screenshots_this_run} 帧。"
        self.log.warning(f"为 {state.infohash_hex} 创建部分成功结果: {reason}")
        return PartialSuccessResult(
            infohash=state.infohash_hex, screenshots_count=screenshots_this_run,
            reason=reason, resume_data=rich_resume_data
        )

    # --- 其他辅助函数 ---

    async def _process_and_generate_screenshot(self, keyframe, extractor, handle, video_file_offset, piece_length, infohash_hex):
        """下载、组装并为单个关键帧生成截图。"""
        try:
            sample = extractor.samples[keyframe.sample_index - 1]
            keyframe_torrent_offset = video_file_offset + sample.offset
            keyframe_piece_indices = self._get_pieces_for_range(keyframe_torrent_offset, sample.size, piece_length)

            keyframe_pieces_data = await self.client.fetch_pieces(handle, keyframe_piece_indices, timeout=60)
            packet_data_bytes = self._assemble_data_from_pieces(keyframe_pieces_data, keyframe_torrent_offset, sample.size, piece_length)

            if len(packet_data_bytes) != sample.size:
                self.log.warning(f"关键帧 {keyframe.index} 的数据不完整，跳过。")
                return False

            packet_data = self._convert_to_annexb(packet_data_bytes, extractor)
            timestamp_str = self._format_timestamp(keyframe)

            await self.generator.generate(
                extradata=extractor.extradata, packet_data=packet_data,
                infohash_hex=infohash_hex, timestamp_str=timestamp_str
            )
            return True
        except Exception as e:
            self.log.error(f"处理关键帧 {keyframe.index} 失败: {e}", exc_info=True)
            return False

    async def _worker(self):
        """工作协程，从队列中循环拉取并处理任务。"""
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("截图工作循环中发生严重未处理错误。")

    def _find_largest_mp4_file(self, ti) -> Optional[Dict[str, Any]]:
        """在 torrent 中查找最大的 .mp4 文件。"""
        largest_file = {'index': -1, 'size': -1, 'offset': -1}
        fs = ti.files()
        for i in range(fs.num_files()):
            if fs.file_path(i).lower().endswith('.mp4') and fs.file_size(i) > largest_file['size']:
                largest_file = {'index': i, 'size': fs.file_size(i), 'offset': fs.file_offset(i)}
        return largest_file if largest_file['index'] != -1 else None

    async def _get_moov_atom_data(self, handle, video_file_offset, video_file_size, piece_length):
        """智能地查找并获取 moov atom 数据。"""
        probe_size = 256 * 1024
        head_size = min(probe_size, video_file_size)
        head_pieces = self._get_pieces_for_range(video_file_offset, head_size, piece_length)
        head_data = await self._fetch_and_assemble(handle, head_pieces, video_file_offset, head_size, piece_length)

        for box_type, partial_box_data, box_offset, box_size in self._parse_mp4_boxes(io.BytesIO(head_data)):
            if box_type == 'moov':
                if len(partial_box_data) < box_size:
                    full_moov_offset = video_file_offset + box_offset
                    needed = self._get_pieces_for_range(full_moov_offset, box_size, piece_length)
                    return await self._fetch_and_assemble(handle, needed, full_moov_offset, box_size, piece_length)
                return partial_box_data
            if box_type == 'mdat' and box_size > video_file_size * 0.8:
                break

        tail_probe_size = 10 * 1024 * 1024
        tail_offset = max(0, video_file_size - tail_probe_size)
        tail_torrent_offset = video_file_offset + tail_offset
        tail_size = min(tail_probe_size, video_file_size - tail_offset)
        tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)
        tail_data = await self._fetch_and_assemble(handle, tail_pieces, tail_torrent_offset, tail_size, piece_length)

        search_pos = len(tail_data)
        while search_pos > 4:
            found_pos = tail_data.rfind(b'moov', 0, search_pos)
            if found_pos == -1: break
            try:
                stream = io.BytesIO(tail_data)
                stream.seek(found_pos - 4)
                box_type, full_box_data, _, _ = next(self._parse_mp4_boxes(stream), (None, None, None, None))
                if box_type == 'moov': return full_box_data
            except Exception: pass
            search_pos = found_pos
        return None

    async def _fetch_and_assemble(self, handle, pieces, offset, size, piece_length):
        """获取 piece 并组装成所需的数据块。"""
        pieces_data = await self.client.fetch_pieces(handle, pieces, timeout=120)
        return self._assemble_data_from_pieces(pieces_data, offset, size, piece_length)

    def _get_pieces_for_range(self, offset, size, piece_length):
        if size <= 0: return []
        return list(range(offset // piece_length, (offset + size - 1) // piece_length + 1))

    def _assemble_data_from_pieces(self, pieces_data, offset, size, piece_length):
        buffer = bytearray(size)
        start_piece, end_piece = offset // piece_length, (offset + size - 1) // piece_length
        buffer_offset = 0
        for i in range(start_piece, end_piece + 1):
            chunk_data = pieces_data.get(i)
            if not chunk_data: continue

            start_in_chunk = offset % piece_length if i == start_piece else 0
            end_in_chunk = (offset + size - 1) % piece_length + 1 if i == end_piece else piece_length

            data_to_copy = chunk_data[start_in_chunk:end_in_chunk]
            copy_len = min(len(data_to_copy), size - buffer_offset)
            if copy_len > 0:
                buffer[buffer_offset:buffer_offset + copy_len] = data_to_copy[:copy_len]
                buffer_offset += copy_len
        return bytes(buffer)

    def _parse_mp4_boxes(self, stream: io.BytesIO) -> Generator[Tuple[str, bytes, int, int], None, None]:
        # ... (此函数的实现保持不变)
        pass

    def _convert_to_annexb(self, packet_data_bytes, extractor):
        if extractor.mode == 'avc1':
            annexb, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
            while cursor < len(packet_data_bytes):
                nal_length = int.from_bytes(packet_data_bytes[cursor : cursor + extractor.nal_length_size], 'big')
                cursor += extractor.nal_length_size
                annexb.extend(start_code + packet_data_bytes[cursor : cursor + nal_length])
                cursor += nal_length
            return bytes(annexb)
        return packet_data_bytes

    def _format_timestamp(self, keyframe: Keyframe) -> str:
        ts_sec = keyframe.pts / keyframe.timescale if keyframe.timescale > 0 else keyframe.index
        m, s = divmod(ts_sec, 60); h, m = divmod(m, 60)
        return f"{int(h):02d}-{int(m):02d}-{int(s):02d}"

    def _check_simulated_failure(self, count: int):
        fail_after = getattr(self, 'FAIL_AFTER_N_KEYFRAMES', None)
        if fail_after is not None and count >= fail_after:
            raise asyncio.TimeoutError("为测试模拟的失败")

    async def _execute_callback(self, callback, result):
        if asyncio.iscoroutinefunction(callback):
            await callback(result)
        else:
            callback(result)
