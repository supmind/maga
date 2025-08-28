# -*- coding: utf-8 -*-
"""
ScreenshotService 的单元测试（适配V2重构）。
"""
import pytest
import asyncio
import base64
from unittest.mock import MagicMock, AsyncMock, patch
import pytest_asyncio

from screenshot.service import (
    ScreenshotService,
    AllSuccessResult,
    FatalErrorResult,
    PartialSuccessResult,
)
from screenshot.extractor import Keyframe, SampleInfo

# --- 辅助函数的单元测试 ---

@pytest.fixture
def service_helpers():
    """为辅助函数测试提供一个简单的服务实例。"""
    return ScreenshotService(loop=None)

class TestServiceHelpers:
    """测试 ScreenshotService 中的各种辅助方法。"""
    def test_get_pieces_for_range(self, service_helpers):
        """测试 _get_pieces_for_range 方法的 piece 计算逻辑。"""
        # 修复：方法签名已更改
        assert service_helpers._get_pieces_for_range(offset=500, size=100, piece_length=1000) == [0]
        assert service_helpers._get_pieces_for_range(offset=900, size=200, piece_length=1000) == [0, 1]
        assert service_helpers._get_pieces_for_range(offset=1500, size=2000, piece_length=1000) == [1, 2, 3]
        assert service_helpers._get_pieces_for_range(offset=2000, size=500, piece_length=1000) == [2]
        assert service_helpers._get_pieces_for_range(offset=1500, size=500, piece_length=1000) == [1]
        assert service_helpers._get_pieces_for_range(offset=1500, size=0, piece_length=1000) == []

    def test_assemble_data_from_pieces(self, service_helpers):
        """测试 _assemble_data_from_pieces 的逻辑。"""
        # 测试从单个 piece 中组装数据
        result_single = service_helpers._assemble_data_from_pieces({10: b'a'*500 + b'B'*100 + b'c'*424}, 10740, 100, 1024)
        assert result_single == b'B' * 100

        # 测试从跨越多个 piece 的数据中组装
        pieces_data_multi = {10: b'a'*1000 + b'B'*24, 11: b'C'*100 + b'd'*924}
        result_multi = service_helpers._assemble_data_from_pieces(pieces_data_multi, 11240, 124, 1024)
        assert result_multi == b'B' * 24 + b'C' * 100

        # 测试当 piece 数据不完整时
        pieces_data_incomplete = {10: b'a'*1000 + b'B'*24}
        result_incomplete = service_helpers._assemble_data_from_pieces(pieces_data_incomplete, 11240, 124, 1024)
        assert result_incomplete == b'B' * 24 + b'\x00' * 100

class TestServiceRefactoredHelpers:
    """为重构后新增的辅助函数提供单元测试。"""
    def test_find_largest_mp4_file(self, service_helpers):
        mock_ti = MagicMock()
        mock_fs = MagicMock()
        mock_ti.files.return_value = mock_fs

        mock_fs.num_files.return_value = 3
        mock_fs.file_path.side_effect = ['a.txt', 'video.mp4', 'video_large.mp4']
        mock_fs.file_size.side_effect = [100, 1000, 2000]
        mock_fs.file_offset.side_effect = [0, 100, 1100]
        result = service_helpers._find_largest_mp4_file(mock_ti)
        assert result['size'] == 2000 and result['index'] == 2

        mock_fs.file_path.side_effect = ['a.txt', 'b.zip', 'c.rar']
        mock_fs.file_size.side_effect = [100, 1000, 2000]
        assert service_helpers._find_largest_mp4_file(mock_ti) is None

    def test_select_keyframes(self, service_helpers):
        mock_extractor = MagicMock()
        mock_extractor.timescale = 90000

        # 场景1: 正常选择
        mock_extractor.keyframes = [Keyframe(i, i, i * 180 * 90000, 90000) for i in range(100)]
        mock_extractor.samples = [MagicMock(pts=100 * 180 * 90000)]
        selected = service_helpers._select_keyframes(mock_extractor)
        assert len(selected) == 100 / 180 * 180 # 期望选择100个截图

        # 场景2: 关键帧数量少于下限
        mock_extractor.keyframes = [Keyframe(i, i, i, 1) for i in range(3)]
        mock_extractor.samples = [MagicMock(pts=1)]
        selected = service_helpers._select_keyframes(mock_extractor)
        assert len(selected) == 3

        # 场景3: 选择数量超过上限
        mock_extractor.keyframes = [Keyframe(i, i, i * 60, 1) for i in range(1000)]
        mock_extractor.samples = [MagicMock(pts=60 * 1000)]
        selected = service_helpers._select_keyframes(mock_extractor)
        assert len(selected) <= 50

# --- 核心业务逻辑的单元测试 ---

@pytest_asyncio.fixture
async def mock_service():
    """提供一个带有模拟依赖的 ScreenshotService 实例。"""
    with patch('screenshot.service.TorrentClient') as MockClient, \
         patch('screenshot.service.H264KeyframeExtractor') as MockExtractor, \
         patch('screenshot.service.ScreenshotGenerator') as MockGenerator:

        mock_client = MockClient.return_value
        mock_extractor_cls = MockExtractor
        mock_generator = MockGenerator.return_value

        mock_handle = MagicMock()
        mock_ti = MagicMock()
        mock_fs = MagicMock()
        mock_fs.num_files.return_value = 1
        mock_fs.file_path.return_value = "video.mp4"
        mock_fs.file_size.return_value = 100000
        mock_ti.files.return_value = mock_fs
        mock_ti.piece_length.return_value = 16384

        # 修复: 确保所有 await 的方法都是 AsyncMock
        mock_client.add_torrent = AsyncMock(return_value=mock_handle)
        mock_client.fetch_pieces = AsyncMock(return_value={})
        mock_client.request_pieces = AsyncMock() # 修复 TypeError
        mock_client.finished_piece_queue = asyncio.Queue()

        mock_extractor_inst = mock_extractor_cls.return_value
        mock_extractor_inst.keyframes = [Keyframe(i, i+1, i*1000, 90000) for i in range(3)]
        mock_extractor_inst.samples = [SampleInfo(i*100, 100, True, i+1, i*1000) for i in range(3)]

        service = ScreenshotService(loop=asyncio.get_running_loop())
        service.client = mock_client
        service.generator = mock_generator

        # 关键修复：模拟（Patch）新的异步辅助方法
        service._get_torrent_info_async = AsyncMock(return_value=mock_ti)
        service._get_moov_atom_data = AsyncMock(return_value=b'mock_moov_data')

        yield service, mock_client, mock_extractor_cls, mock_generator, mock_handle

@pytest.mark.asyncio
class TestServiceOrchestration:
    """测试 ScreenshotService 的核心业务流程和状态管理。"""

    async def test_fatal_error_if_moov_fails(self, mock_service):
        service, _, _, _, mock_handle = mock_service
        service._get_moov_atom_data.side_effect = Exception("模拟 MOOV 获取失败")
        result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")
        assert isinstance(result, FatalErrorResult)
        # 修复：断言与新的错误信息匹配
        assert "初始化任务失败: 模拟 MOOV 获取失败" in result.reason

    async def test_fatal_error_if_no_mp4_file_found(self, mock_service):
        service, _, _, _, mock_handle = mock_service
        mock_ti = await service._get_torrent_info_async(mock_handle)
        mock_ti.files.return_value.num_files.return_value = 0 # 模拟没有文件
        result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")
        assert isinstance(result, FatalErrorResult)
        assert "无法初始化任务状态" in result.reason

    async def test_partial_success_on_piece_timeout(self, mock_service):
        service, mock_client, _, _, mock_handle = mock_service
        mock_client.finished_piece_queue.get = AsyncMock(side_effect=asyncio.TimeoutError)
        result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")
        assert isinstance(result, PartialSuccessResult)
        assert "等待 pieces 超时" in result.reason
        assert result.screenshots_count == 0

    async def test_full_success_scenario(self, mock_service):
        service, mock_client, _, _, mock_handle = mock_service
        # 模拟 piece 队列返回所需的所有 piece
        for i in range(10): # 假设需要10个 piece
             await mock_client.finished_piece_queue.put(i)

        # 确保循环可以终止
        mock_client.finished_piece_queue.get.side_effect = asyncio.TimeoutError

        with patch.object(service, '_build_piece_download_plan') as mock_build_plan:
            # 模拟一个简单的下载计划
            mock_build_plan.return_value = ({0: {'keyframe': Keyframe(0,1,0,1), 'needed_pieces': {0}}}, {0: {0}}, {0})

            with patch.object(service, '_process_and_generate_screenshot', new_callable=AsyncMock) as mock_process:
                mock_process.return_value = True
                result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")

                assert isinstance(result, AllSuccessResult)
                assert result.screenshots_count == 1
                mock_process.assert_called_once()
