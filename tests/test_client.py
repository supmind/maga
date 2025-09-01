# -*- coding: utf-8 -*-
"""
对 TorrentClient 的集成测试。

这些测试模拟 libtorrent 的行为，以验证 TorrentClient 的异步逻辑、
状态管理和错误处理是否正确。
"""
import asyncio
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from screenshot.client import TorrentClient, MetadataTimeoutError
from screenshot.errors import TorrentClientError

# 将此文件中的所有测试标记为异步测试
pytestmark = pytest.mark.asyncio

@pytest.fixture
def mock_lt():
    """全面地模拟 libtorrent 库，包括 session 和 alert。"""
    with patch('screenshot.client.lt') as mock_lt_lib:
        mock_session = MagicMock()
        mock_lt_lib.session.return_value = mock_session
        mock_lt_lib.parse_magnet_uri.return_value = MagicMock()

        # 为警报类型创建 Mock
        mock_lt_lib.alert_category = MagicMock()
        mock_lt_lib.metadata_received_alert = MagicMock()
        mock_lt_lib.piece_finished_alert = MagicMock()
        mock_lt_lib.read_piece_alert = MagicMock()

        yield mock_lt_lib

def create_mock_alert(handle):
    """辅助函数，用于创建带有 handle 的模拟警报。"""
    alert = MagicMock()
    alert.handle = handle
    return alert

def setup_client(mock_lt, loop, timeout=30):
    """辅助函数，用于创建和配置一个测试用的 client 实例。"""
    client = TorrentClient(loop=loop, metadata_timeout=timeout)
    client._ses = mock_lt.session.return_value
    client.start = AsyncMock()
    client.stop = AsyncMock()
    return client

async def test_get_handle_adds_and_removes_torrent(mock_lt):
    """测试 get_handle 上下文管理器是否能正确添加和移除 torrent。"""
    loop = asyncio.get_running_loop()
    client = setup_client(mock_lt, loop)

    infohash = "testhash123456789012345678901234567890"
    mock_handle = MagicMock()
    mock_handle.info_hash.return_value = infohash
    mock_handle.is_valid.return_value = True

    client.add_torrent = AsyncMock(return_value=mock_handle)
    client.remove_torrent = AsyncMock()

    async with client.get_handle(infohash):
        client.add_torrent.assert_called_once_with(infohash, metadata=None)

    client.remove_torrent.assert_called_once_with(mock_handle)

async def test_add_torrent_metadata_success(mock_lt):
    """测试当收到 metadata_received_alert 时，元数据下载的 Future 会被正确解析。"""
    loop = asyncio.get_running_loop()
    client = setup_client(mock_lt, loop)

    infohash = "metadatasuccesshash123456789012345678"
    mock_handle = MagicMock()
    mock_handle.info_hash.return_value = infohash

    mock_lt.session.return_value.add_torrent.return_value = mock_handle

    add_task = asyncio.create_task(client.add_torrent(infohash))
    await asyncio.sleep(0.01)

    assert infohash in client.pending_metadata
    future = client.pending_metadata[infohash]
    assert not future.done()

    mock_alert = create_mock_alert(mock_handle)
    client._handle_metadata_received(mock_alert)

    returned_handle = await add_task

    assert returned_handle == mock_handle
    assert infohash not in client.pending_metadata

async def test_add_torrent_metadata_timeout(mock_lt):
    """测试在未收到元数据警报时，add_torrent 会因超时而失败。"""
    loop = asyncio.get_running_loop()
    client_with_timeout = setup_client(mock_lt, loop, timeout=0.1)

    infohash = "metadatatimeouthash12345678901234567"
    mock_lt.session.return_value.add_torrent.return_value = MagicMock()

    with pytest.raises(MetadataTimeoutError, match="获取元数据超时"):
        await client_with_timeout.add_torrent(infohash)

async def test_fetch_pieces_success(mock_lt):
    """测试 fetch_pieces 的完整成功流程：下载 -> 读取 -> 返回数据。"""
    loop = asyncio.get_running_loop()
    client = setup_client(mock_lt, loop)

    infohash = "fetchsuccesshash1234567890123456789"
    mock_handle = MagicMock()
    mock_handle.info_hash.return_value = infohash
    mock_handle.have_piece.return_value = False

    piece_indices = [5, 10]

    fetch_task = asyncio.create_task(client.fetch_pieces(mock_handle, piece_indices, timeout=1))
    await asyncio.sleep(0.01)

    assert len(client.pending_fetches) == 1
    fetch_id = list(client.pending_fetches.keys())[0]
    fetch_future = client.pending_fetches[fetch_id]['future']
    assert not fetch_future.done()

    alert1 = create_mock_alert(mock_handle)
    alert1.piece_index = 5
    client._handle_piece_finished(alert1)

    alert2 = create_mock_alert(mock_handle)
    alert2.piece_index = 10
    client._handle_piece_finished(alert2)

    await asyncio.sleep(0.01)
    assert fetch_future.done()

    assert len(client.pending_reads) == 2

    read_alert1 = create_mock_alert(mock_handle)
    read_alert1.piece = 5
    read_alert1.buffer = b"piece_5_data"
    read_alert1.error = None
    client._handle_read_piece(read_alert1)

    read_alert2 = create_mock_alert(mock_handle)
    read_alert2.piece = 10
    read_alert2.buffer = b"piece_10_data"
    read_alert2.error = None
    client._handle_read_piece(read_alert2)

    result = await fetch_task

    assert result == {
        5: b"piece_5_data",
        10: b"piece_10_data"
    }
    assert not client.pending_fetches
    assert not client.pending_reads

async def test_fetch_pieces_cancellation_cleans_up_pending_reads(mock_lt):
    """验证取消 fetch_pieces 调用能正确地从 pending_reads 字典中移除对应的 future。"""
    loop = asyncio.get_running_loop()
    client = setup_client(mock_lt, loop)

    mock_handle = MagicMock()
    mock_handle.is_valid.return_value = True
    mock_handle.have_piece.return_value = True
    mock_handle.info_hash.return_value = "mockinfohash12345678901234567890"

    piece_indices = [0, 1, 2]
    infohash_hex = str(mock_handle.info_hash())

    fetch_task = asyncio.create_task(client.fetch_pieces(mock_handle, piece_indices))
    await asyncio.sleep(0.01)

    assert len(client.pending_reads) == 3
    future_for_piece_0 = client.pending_reads[(infohash_hex, 0)]['future']

    fetch_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await fetch_task

    assert len(client.pending_reads) == 0
    assert future_for_piece_0.cancelled()
