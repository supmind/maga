# -*- coding: utf-8 -*-
"""
对 screenshot.client.TorrentClient 的健壮性测试（V3 - 修复了测试替身）。

这些测试旨在验证并发场景、资源管理（LRU缓存）和错误处理，
确保客户端在面对现实世界的复杂情况时不会死锁、超时或行为不当。
"""
import pytest
import pytest_asyncio
import asyncio
import libtorrent as lt
import threading
from unittest.mock import MagicMock, patch, AsyncMock

from screenshot.client import TorrentClient, LibtorrentError, _TorrentRequest

# --- 模拟 Libtorrent 对象 ---

def create_mock_handle(infohash_hex: str):
    handle = MagicMock(spec=lt.torrent_handle)
    handle.is_valid.return_value = True
    handle.info_hash.return_value = lt.sha1_hash(bytes.fromhex(infohash_hex))
    ti = MagicMock(spec=lt.torrent_info)
    ti.num_pieces.return_value = 100
    handle.get_torrent_info.return_value = ti
    return handle

def create_mock_alert(alert_type, handle, **kwargs):
    alert = MagicMock(spec=alert_type)
    alert.category.return_value = 0
    alert.handle = handle
    for key, value in kwargs.items():
        setattr(alert, key, value)
    alert.__class__ = alert_type
    return alert

# --- Pytest Fixtures ---

@pytest_asyncio.fixture
async def mock_lt_session():
    """一个经过修正的、能正确模拟 libtorrent 阻塞行为的 session 替身。"""
    with patch('libtorrent.session') as mock_session_class:
        mock_ses = mock_session_class.return_value

        alert_posted = threading.Event()
        mock_ses.posted_alerts = []

        def wait_for_alert_mock(timeout):
            alert_posted.wait(timeout)

        def pop_alerts_mock():
            alerts = mock_ses.posted_alerts
            mock_ses.posted_alerts = []
            alert_posted.clear()
            return alerts

        def post_alert_sync(alert):
            mock_ses.posted_alerts.append(alert)
            alert_posted.set()

        mock_ses.wait_for_alert.side_effect = wait_for_alert_mock
        mock_ses.pop_alerts.side_effect = pop_alerts_mock
        mock_ses.post_alert = post_alert_sync

        yield mock_ses

@pytest_asyncio.fixture
async def client(mock_lt_session):
    """提供一个已启动并正在运行的 TorrentClient 实例及其模拟会话。"""
    torrent_client = TorrentClient(loop=asyncio.get_running_loop(), max_cache_size=2)
    torrent_client.ses = mock_lt_session

    await torrent_client.start()
    yield torrent_client, mock_lt_session
    torrent_client.stop()
    await asyncio.sleep(0.05)

# --- 完整测试用例 ---

@pytest.mark.asyncio
class TestTorrentClientFinal:
    """TorrentClient 核心功能的最终测试套件。"""

    async def test_add_torrent_success(self, client):
        torrent_client, mock_ses = client
        infohash = "a" * 40
        mock_handle = create_mock_handle(infohash)
        mock_ses.add_torrent.return_value = mock_handle

        add_task = asyncio.create_task(torrent_client.add_torrent(infohash))
        await asyncio.sleep(0.01)

        mock_ses.post_alert(create_mock_alert(lt.metadata_received_alert, handle=mock_handle))

        handle = await asyncio.wait_for(add_task, timeout=1)
        assert handle is not None
        handle.pause.assert_called_once()

    async def test_add_torrent_timeout(self, client):
        torrent_client, mock_ses = client
        infohash = "b" * 40
        mock_ses.add_torrent.return_value = create_mock_handle(infohash)

        with pytest.raises(LibtorrentError, match="获取元数据超时"):
            await torrent_client.add_torrent(infohash, timeout=0.1)

    async def test_fetch_pieces_success(self, client):
        torrent_client, mock_ses = client
        infohash = "c" * 40
        mock_handle = create_mock_handle(infohash)
        mock_handle.have_piece.return_value = False

        req = _TorrentRequest(mock_handle, torrent_client.loop)
        torrent_client.requests[infohash] = req
        req.read_piece = AsyncMock(side_effect=[b'p0', b'p1'])

        fetch_task = asyncio.create_task(torrent_client.fetch_pieces(mock_handle, [0, 1]))
        await asyncio.sleep(0.01)

        mock_ses.post_alert(create_mock_alert(lt.piece_finished_alert, handle=mock_handle, piece_index=0))
        mock_ses.post_alert(create_mock_alert(lt.piece_finished_alert, handle=mock_handle, piece_index=1))

        result = await asyncio.wait_for(fetch_task, timeout=1)
        assert result == {0: b'p0', 1: b'p1'}

    async def test_fetch_pieces_timeout(self, client):
        torrent_client, mock_ses = client
        infohash = "d" * 40
        mock_handle = create_mock_handle(infohash)
        mock_handle.have_piece.return_value = False
        torrent_client.requests[infohash] = _TorrentRequest(mock_handle, torrent_client.loop)

        with pytest.raises(LibtorrentError, match="下载或读取 pieces .* 超时"):
            await torrent_client.fetch_pieces(mock_handle, [5, 6], timeout=0.1)

    async def test_lru_eviction(self, client):
        torrent_client, mock_ses = client
        h1_info, h2_info, h3_info = "a1" * 20, "a2" * 20, "a3" * 20
        h1, h2, h3 = create_mock_handle(h1_info), create_mock_handle(h2_info), create_mock_handle(h3_info)
        mock_ses.add_torrent.side_effect = [h1, h2, h3]

        # 添加 h1, h2
        add_task1 = asyncio.create_task(torrent_client.add_torrent(h1_info))
        await asyncio.sleep(0); mock_ses.post_alert(create_mock_alert(lt.metadata_received_alert, handle=h1)); await add_task1

        add_task2 = asyncio.create_task(torrent_client.add_torrent(h2_info))
        await asyncio.sleep(0); mock_ses.post_alert(create_mock_alert(lt.metadata_received_alert, handle=h2)); await add_task2

        assert list(torrent_client.requests.keys()) == [h1_info, h2_info]

        # 添加 h3, 触发驱逐 h1
        add_task3 = asyncio.create_task(torrent_client.add_torrent(h3_info))
        await asyncio.sleep(0); mock_ses.post_alert(create_mock_alert(lt.metadata_received_alert, handle=h3)); await add_task3

        assert list(torrent_client.requests.keys()) == [h2_info, h3_info]
        mock_ses.remove_torrent.assert_called_once_with(h1, lt.session.delete_files)

    async def test_remove_torrent_cleans_up(self, client):
        torrent_client, mock_ses = client
        infohash = "ee" * 20
        mock_handle = create_mock_handle(infohash)
        mock_ses.add_torrent.return_value = mock_handle

        add_task = asyncio.create_task(torrent_client.add_torrent(infohash))
        await asyncio.sleep(0); mock_ses.post_alert(create_mock_alert(lt.metadata_received_alert, handle=mock_handle)); await add_task

        assert infohash in torrent_client.requests
        await torrent_client.remove_torrent(mock_handle)
        assert infohash not in torrent_client.requests
        mock_ses.remove_torrent.assert_called_once_with(mock_handle, lt.session.delete_files)
