# -*- coding: utf-8 -*-
"""
对 screenshot/service.py 核心服务逻辑的单元测试。
"""
import pytest
import io
import struct
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from worker.screenshot.service import ScreenshotService, StatusCallback
from config import Settings
from worker.screenshot.errors import MP4ParsingError, NoVideoFileError, FrameDownloadTimeoutError, MoovNotFoundError
from worker.screenshot.extractor import Keyframe, SampleInfo, KeyframeExtractor
from worker.screenshot.client import TorrentClient


# --- 新增的测试 ---

class TestAssembleData:
    """针对 ScreenshotService._assemble_data_from_pieces 的专用测试类。"""

    @pytest.fixture
    def service(self, settings):
        """提供一个无 mock 依赖的 ScreenshotService 实例，用于测试独立函数。"""
        return ScreenshotService(settings=settings)

    def test_assemble_from_single_piece(self, service):
        """测试所需数据完全包含在单个 piece 中的情况。"""
        pieces_data = {0: b"0123456789"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=2, size=5, piece_length=10)
        assert result == b"23456"

    def test_assemble_spanning_two_pieces(self, service):
        """测试所需数据跨越两个 piece 的情况。"""
        pieces_data = {0: b"0123456789", 1: b"abcdefghij"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=4, piece_length=10)
        assert result == b"89ab"

    def test_assemble_spanning_multiple_pieces(self, service):
        """测试所需数据跨越多个 piece 的情况。"""
        pieces_data = {
            0: b"0123456789",
            1: b"abcdefghij",
            2: b"KLMNOPQRST"
        }
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=15, piece_length=10)
        assert result == b"89abcdefghijKLM"

    def test_assemble_at_piece_boundary(self, service):
        """测试所需数据正好在 piece 边界上的情况。"""
        pieces_data = {0: b"0123456789", 1: b"abcdefghij"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=10, size=5, piece_length=10)
        assert result == b"abcde"

    def test_assemble_with_missing_piece(self, service):
        """测试当缺少所需 piece 时，函数应返回空字节。"""
        pieces_data = {0: b"0123456789", 2: b"KLMNOPQRST"} # 故意缺少 piece 1
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=15, piece_length=10)
        assert result == b""


def test_serialize_task_state(service):
    """测试 _serialize_task_state 方法是否能正确地将一个复杂的任务状态对象转换为字典。"""
    # 1. 创建模拟的 extractor 和其他状态数据
    mock_extractor = MagicMock(spec=KeyframeExtractor)
    mock_extractor.extradata = b"some_bytes"
    mock_extractor.codec_name = "h264"
    mock_extractor.mode = "avc1"
    mock_extractor.nal_length_size = 4
    mock_extractor.timescale = 90000
    mock_extractor.samples = [SampleInfo(1, 100, True, 1, 90000)]

    task_state = {
        'infohash': 'test_hash',
        'piece_length': 16384,
        'video_file_offset': 0,
        'video_file_size': 1000,
        'extractor': mock_extractor,
        'all_keyframes': [Keyframe(0, 1, 90000, 90000)],
        'selected_keyframes': [Keyframe(0, 1, 90000, 90000)],
        'completed_pieces': {0, 1, 2},
        'processed_keyframes': {0}
    }

    # 2. 调用序列化方法
    serialized_data = service._serialize_task_state(task_state)

    # 3. 验证结果
    assert serialized_data is not None
    assert serialized_data['infohash'] == 'test_hash'
    assert serialized_data['extractor_info']['extradata'] == b"some_bytes"
    assert len(serialized_data['all_keyframes']) == 1
    assert serialized_data['all_keyframes'][0]['sample_index'] == 1
    assert list(serialized_data['processed_keyframes']) == [0]


# --- 原有的测试 Fixtures 和 Cases ---

@pytest.fixture
def settings():
    """提供一个默认的 Settings 对象。"""
    return Settings(min_screenshots=2, max_screenshots=5, default_screenshots=3, target_interval_sec=60)

@pytest.fixture
def mock_callbacks():
    """提供 mock 的状态和截图回调函数，并包含一个 Future 用于同步。"""
    future = asyncio.Future()
    async def status_callback(*args, **kwargs):
        if not future.done():
            future.set_result(kwargs)
    return {"status_callback": status_callback, "future": future}

@pytest.fixture
def service(settings, mock_callbacks):
    """提供一个依赖已被 mock 的 ScreenshotService 实例。"""
    loop = asyncio.get_event_loop()
    with patch('worker.screenshot.service.TorrentClient'), patch('worker.screenshot.service.ScreenshotGenerator'):
        service_instance = ScreenshotService(
            settings=settings, loop=loop, status_callback=mock_callbacks["status_callback"]
        )
        yield service_instance

# --- Test Cases ---

def test_select_keyframes_logic(service):
    """
    测试新的 _select_keyframes 逻辑，确保它是根据时间戳均匀选择，
    而不是根据关键帧在列表中的索引。
    """
    # 1. 创建一组时间戳分布不均的关键帧
    # Keyframe(index, sample_index, pts, timescale)
    all_keyframes = [
        Keyframe(0, 0, 0, 90000),      # 0s
        Keyframe(1, 1, 10 * 90000, 90000), # 10s
        Keyframe(2, 2, 20 * 90000, 90000), # 20s
        Keyframe(3, 3, 88 * 90000, 90000), # 88s
        Keyframe(4, 4, 95 * 90000, 90000), # 95s
        Keyframe(5, 5, 170 * 90000, 90000) # 170s
    ]

    # 视频总时长为 180s
    duration_pts = 180 * 90000

    # 根据 settings (min=2, max=5, interval=60), 180s 的视频应该生成 180/60 = 3 张截图
    # 目标时间点应该是: 0s, 60s, 120s

    # 2. 调用被测方法
    selected = service._select_keyframes(all_keyframes, 90000, duration_pts, None)

    # 3. 断言
    assert len(selected) == 3

    selected_pts = [kf.pts for kf in selected]

    # 验证逻辑：
    # 目标时间点 (PTS): [0, 5400000, 10800000] (0s, 60s, 120s)
    #
    # target = 0 -> closest is 0
    # target = 5400000 (60s) -> closest is 88s (PTS 7920000), diff=28s.
    # target = 10800000 (120s) -> closest is 95s (PTS 8550000), diff=25s.
    expected_pts = [0, 88 * 90000, 95 * 90000]
    assert sorted(selected_pts) == sorted(expected_pts)

@patch('worker.screenshot.service.KeyframeExtractor')
def test_load_state_from_resume_data(MockKeyframeExtractor, service):
    """测试从 resume_data 恢复任务状态的逻辑。"""
    mock_extractor_instance = MockKeyframeExtractor.return_value
    resume_data = {
        "infohash": "r_hash", "piece_length": 2, "video_file_offset": 3, "video_file_size": 4,
        "extractor_info": { "extradata": "AQIDBA==", "codec_name": "h264", "mode": "avc1", "nal_length_size": 4, "timescale": 90000,
            "samples": [{"offset": 1, "size": 1, "is_keyframe": True, "index": 1, "pts": 0}] },
        "all_keyframes": [{"index": 0, "sample_index": 1, "pts": 0, "timescale": 90000}],
        "selected_keyframes": [{"index": 0, "sample_index": 1, "pts": 0, "timescale": 90000}],
        "completed_pieces": [1, 2], "processed_keyframes": []
    }
    state = service._load_state_from_resume_data(resume_data)
    assert state["extractor"].extradata == b'\x01\x02\x03\x04'

@pytest.mark.asyncio
@patch.object(ScreenshotService, '_generate_screenshots_from_torrent', new_callable=AsyncMock)
async def test_handle_task_permanent_failure(mock_generate, service, mock_callbacks):
    """测试当子流程抛出永久性错误时，任务是否被正确处理。"""
    mock_generate.side_effect = NoVideoFileError("Not found", "no_video_hash")

    await service._handle_screenshot_task({"infohash": "no_video_hash"})

    result = await asyncio.wait_for(mock_callbacks["future"], timeout=1)
    assert result.get("status") == "permanent_failure"
    assert isinstance(result.get("error"), NoVideoFileError)

@pytest.mark.asyncio
@patch.object(ScreenshotService, '_generate_screenshots_from_torrent', new_callable=AsyncMock)
async def test_handle_task_recoverable_failure(mock_generate, service, mock_callbacks):
    """测试当子流程抛出可恢复错误时，任务是否被正确处理。"""
    error = FrameDownloadTimeoutError("Timeout", "recoverable_hash", resume_data={"key": "value"})
    mock_generate.side_effect = error

    await service._handle_screenshot_task({"infohash": "recoverable_hash"})

    result = await asyncio.wait_for(mock_callbacks["future"], timeout=1)
    assert result.get("status") == "recoverable_failure"
    assert result.get("resume_data") == {"key": "value"}

@pytest.mark.asyncio
@patch.object(ScreenshotService, '_generate_screenshots_from_torrent', new_callable=AsyncMock)
async def test_handle_task_successful_run(mock_generate, service, mock_callbacks):
    """测试截图任务的完整成功路径。"""
    infohash = "success_hash"

    await service._handle_screenshot_task({"infohash": infohash})

    result = await asyncio.wait_for(mock_callbacks["future"], timeout=1)
    assert result.get("status") == "success"
    mock_generate.assert_awaited_once()

@pytest.mark.asyncio
async def test_get_moov_atom_fetches_full_box_on_partial_find(service):
    """
    测试当 _get_moov_atom_data 在头部探测中找到一个部分的 'moov' box 时，
    它是否能正确地触发一次额外的下载来获取完整的 box。
    """
    # 1. 模拟 service 的依赖
    mock_handle = MagicMock()
    service.client.fetch_pieces = AsyncMock()

    # 2. 设置场景数据
    # 一个部分的 moov box，头部声明大小为 1000，但我们只有 50 字节
    partial_moov_data = b'\x00\x00\x03\xe8moov' + b'\x01' * 42 # 头部(8) + 内容(42) = 50 字节
    # 一个完整的 moov box，用于模拟第二次下载的结果
    full_moov_data = b'\x00\x00\x03\xe8moov' + b'\xff' * (1000 - 8)

    # 模拟 fetch_pieces 的行为:
    # - 第一次调用（头部探测）返回部分数据
    # - 第二次调用（为获取完整 moov）返回完整数据
    service.client.fetch_pieces.side_effect = [
        {0: partial_moov_data},
        {1: full_moov_data}
    ]

    # 模拟 _assemble_data_from_pieces 的行为
    def mock_assemble(pieces_data, *args):
        if 0 in pieces_data:
            return partial_moov_data
        if 1 in pieces_data:
            return full_moov_data
        return b""

    # 3. 使用 patch.object 来临时替换实例上的方法并执行
    with patch.object(service, '_assemble_data_from_pieces', side_effect=mock_assemble):
        result_moov_data = await service._get_moov_atom_data(mock_handle, 0, 2000, 512, "partial_moov_hash")

    # 4. 断言
    # 验证返回的是完整的 moov 数据
    assert result_moov_data == full_moov_data
    # 验证 fetch_pieces 被调用了两次
    assert service.client.fetch_pieces.call_count == 2
    # 验证第二次调用是去获取完整的 moov box (大小为 1000)
    second_call_args = service.client.fetch_pieces.call_args_list[1]
    # (handle, pieces_to_fetch, timeout=...)
    pieces_to_fetch = second_call_args[0][1]
    # 假设 box 在偏移量 0 处找到，piece_length=512。大小为1000的box会跨越 piece 0 和 1。
    assert pieces_to_fetch == [0, 1]

@pytest.mark.asyncio
async def test_get_queue_size(service):
    """测试 get_queue_size 方法是否能准确反映内部队列的大小。"""
    # 1. 初始状态下，队列应为空
    assert service.get_queue_size() == 0

    # 2. 向队列中放入一些任务
    await service.task_queue.put("task1")
    await service.task_queue.put("task2")

    # 3. 验证队列大小
    assert service.get_queue_size() == 2

    # 4. 取出一个任务后再次验证
    _ = await service.task_queue.get()
    assert service.get_queue_size() == 1
