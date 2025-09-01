# -*- coding: utf-8 -*-
import asyncio
import struct
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, ANY

from screenshot.service import ScreenshotService, Keyframe, SampleInfo
from screenshot.config import Settings
from screenshot.errors import MetadataTimeoutError, MoovNotFoundError, FrameDownloadTimeoutError

pytestmark = pytest.mark.asyncio

# --- 辅助函数，用于构建测试用的 MP4 Box ---
def create_box(box_type: bytes, payload: bytes) -> bytes:
    """创建一个简单的 MP4 box，使用 32 位大小。"""
    size = 8 + len(payload)
    return struct.pack('>I4s', size, box_type) + payload

def build_test_moov() -> bytes:
    """为测试构建一个简化的、但结构上有效的 'moov' box。"""
    stsd_payload = b'\x00\x00\x00\x00' + b'\x00\x00\x00\x01'
    config_box = create_box(b'avcC', b'\x01\x64\x00\x1f\xff\xe1\x00\x19' + (b'x' * 25))
    visual_sample_entry_header = b'\x00' * 78
    sample_entry_payload = visual_sample_entry_header + config_box
    stsd_payload += create_box(b'avc1', sample_entry_payload)
    stsd = create_box(b'stsd', stsd_payload)
    stts_payload = b'\x00\x00\x00\x00' + b'\x00\x00\x00\x01' + struct.pack('>II', 10, 1000)
    stts = create_box(b'stts', stts_payload)
    stss_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 2) + struct.pack('>II', 1, 5)
    stss = create_box(b'stss', stss_payload)
    stsc_payload = b'\x00\x00\x00\x00' + b'\x00\x00\x00\x01' + struct.pack('>III', 1, 10, 1)
    stsc = create_box(b'stsc', stsc_payload)
    stsz_payload = b'\x00\x00\x00\x00' + struct.pack('>II', 100, 10)
    stsz = create_box(b'stsz', stsz_payload)
    stco_payload = b'\x00\x00\x00\x00' + b'\x00\x00\x00\x01' + struct.pack('>I', 1024)
    stco = create_box(b'stco', stco_payload)
    stbl_payload = stsd + stts + stss + stsc + stsz + stco
    stbl = create_box(b'stbl', stbl_payload)
    minf = create_box(b'minf', stbl)
    # 修复：在 mdhd box 中提供一个有效的 timescale
    mdia_payload = create_box(b'mdhd', b'\x00' * 12 + struct.pack('>I', 90000) + b'\x00' * 8)
    mdia = create_box(b'mdia', mdia_payload)
    trak = create_box(b'trak', create_box(b'tkhd', b'\x00'*84) + mdia)
    moov_payload = create_box(b'mvhd', b'\x00' * 100) + trak
    return create_box(b'moov', moov_payload)


@pytest_asyncio.fixture
async def service(mock_service_dependencies, status_callback):
    """提供一个已启动并注入了所有模拟依赖的 ScreenshotService 实例。"""
    settings = Settings(num_workers=1)
    loop = asyncio.get_running_loop()
    service_instance = ScreenshotService(settings=settings, loop=loop, status_callback=status_callback)

    service_instance.client.start = AsyncMock()
    service_instance.client.stop = AsyncMock()

    await service_instance.run()
    yield service_instance
    await service_instance.stop()

def configure_mock_extractor(extractor_class, keyframe_indices):
    """辅助函数，用于创建一个完整配置的 mock extractor。"""
    mock_extractor = MagicMock()
    mock_keyframes = [Keyframe(index=i, sample_index=s, pts=i*1000, timescale=90000) for i, s in enumerate(keyframe_indices)]
    mock_samples = [SampleInfo(offset=i*100, size=100, is_keyframe=(i+1 in keyframe_indices), index=i+1, pts=i*500) for i in range(10)]

    mock_extractor.keyframes = mock_keyframes
    mock_extractor.samples = mock_samples
    mock_extractor.codec_name = 'h264'
    mock_extractor.extradata = b'extradata'
    mock_extractor.timescale = 90000
    mock_extractor.mode = 'avc1'
    mock_extractor.nal_length_size = 4

    extractor_class.return_value = mock_extractor
    return mock_extractor

async def test_happy_path_success(service, mock_service_dependencies, status_callback):
    """测试一个截图任务从提交到成功完成的完整“愉快路径”。"""
    infohash = "happyhash123456789012345678901234567890"
    client = mock_service_dependencies['client']
    generator = mock_service_dependencies['generator']

    valid_moov_data = build_test_moov()
    keyframe_piece_data = b'keyframe_data'

    async def mock_fetch_pieces(handle, pieces, timeout=None):
        if 0 in pieces:
            return {p: valid_moov_data for p in pieces}
        return {p: keyframe_piece_data for p in pieces}
    client.fetch_pieces.side_effect = mock_fetch_pieces

    configure_mock_extractor(mock_service_dependencies['extractor'], keyframe_indices=[1, 5])

    await service.submit_task(infohash)
    await service.task_queue.join()

    status_callback.assert_called_once_with(status='success', infohash=infohash, message=ANY)
    assert generator.generate.call_count == 2

async def test_metadata_timeout_is_recoverable_failure(service, mock_service_dependencies, status_callback):
    """测试当 TorrentClient 获取元数据超时时，任务会以“可恢复的失败”结束。"""
    infohash = "metatimeouthash12345678901234567890123"
    client = mock_service_dependencies['client']

    client.get_handle.side_effect = MetadataTimeoutError("Timeout!", infohash)

    await service.submit_task(infohash)
    await service.task_queue.join()

    status_callback.assert_called_once()
    _, kwargs = status_callback.call_args
    assert kwargs.get('status') == 'recoverable_failure'
    assert isinstance(kwargs.get('error'), MetadataTimeoutError)

async def test_moov_not_found_is_permanent_failure(service, mock_service_dependencies, status_callback):
    """测试当无法找到 moov box 时，任务会以“永久性失败”结束。"""
    infohash = "moovfailhash1234567890123456789012345"
    client = mock_service_dependencies['client']

    client.fetch_pieces.return_value = {}

    await service.submit_task(infohash)
    await service.task_queue.join()

    status_callback.assert_called_once()
    _, kwargs = status_callback.call_args
    assert kwargs.get('status') == 'permanent_failure'
    assert isinstance(kwargs.get('error'), MoovNotFoundError)

async def test_task_recovery_after_recoverable_failure(service, mock_service_dependencies, status_callback):
    """测试任务恢复流程。"""
    infohash = "resumehash1234567890123456789012345678"
    client = mock_service_dependencies['client']
    extractor_class = mock_service_dependencies['extractor']

    async def mock_fetch_moov_only(handle, pieces, timeout=None):
        return {p: build_test_moov() for p in pieces}
    client.fetch_pieces.side_effect = mock_fetch_moov_only

    # 第一次运行时，需要一个完整的 mock extractor
    configure_mock_extractor(extractor_class, keyframe_indices=[1, 5])

    # 模拟第一次处理 piece 失败
    mock_resume_data = {"infohash": infohash, "extractor_info": {"codec_name": "h264"}, "processed_keyframes": []}
    service._process_keyframe_pieces = AsyncMock(side_effect=FrameDownloadTimeoutError("Timeout!", infohash, resume_data=mock_resume_data))

    await service.submit_task(infohash)
    await service.task_queue.join()

    status_callback.assert_called_once()
    _, kwargs = status_callback.call_args
    assert kwargs['status'] == 'recoverable_failure'
    resume_data = kwargs.get('resume_data')
    assert resume_data is not None

    # --- 第二次运行 ---
    status_callback.reset_mock()
    extractor_class.reset_mock()

    # 模拟第二次成功
    service._process_keyframe_pieces = AsyncMock(return_value=({1}, {1: asyncio.Future()}))

    await service.submit_task(infohash, resume_data=resume_data)
    await service.task_queue.join()

    status_callback.assert_called_once_with(status='success', infohash=infohash, message=ANY)
    extractor_class.assert_not_called()

async def test_task_with_metadata_calls_get_handle_with_metadata(service, mock_service_dependencies, status_callback):
    """测试如果任务附带元数据提交，服务会将该元数据传递给客户端的 get_handle 方法。"""
    infohash = "c824d516243886576b5151b148ef8648344e433a"
    metadata = b'd4:infod4:name4:test12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee'
    client = mock_service_dependencies['client']

    client.fetch_pieces.return_value = {0: build_test_moov()}
    configure_mock_extractor(mock_service_dependencies['extractor'], keyframe_indices=[1])
    service._process_keyframe_pieces = AsyncMock(return_value=({1}, {1: asyncio.Future()}))

    await service.submit_task(infohash=infohash, metadata=metadata)
    await service.task_queue.join()

    client.get_handle.assert_called_once_with(infohash, metadata=metadata)
    status_callback.assert_called_once_with(status='success', infohash=infohash, message=ANY)
