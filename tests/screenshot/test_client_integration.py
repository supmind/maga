# -*- coding: utf-8 -*-
"""
本模块包含 ScreenshotClient 的集成测试。

这些测试会连接到真实的 BitTorrent 网络，因此被标记为 'integration'，
以便可以与常规的单元测试分开运行。
"""
import pytest
import asyncio

from worker.screenshot.client import TorrentClient
from worker.screenshot.errors import MetadataTimeoutError
from config import Settings

# 使用一个已知的、活跃度较高的 infohash 进行测试
# 这是 Ubuntu 22.04.1 Desktop (64-bit) 的 torrent
ACTIVE_INFOHASH = "08ada5a7a6183aae1e09d831df6748d566095a10"
METADATA_FETCH_TIMEOUT = 120  # 为元数据下载设置一个较高的超时时间
PIECE_FETCH_TIMEOUT = 180     # 为数据块下载设置一个较高的超时时间

@pytest.mark.integration
@pytest.mark.asyncio
async def test_torrent_client_fetches_metadata_and_piece():
    """
    测试 TorrentClient 是否能成功从一个活跃的 infohash 中：
    1. 获取元数据 (torrent info)。
    2. 下载并读取至少一个数据块 (piece 0)。
    """
    settings = Settings(metadata_timeout=METADATA_FETCH_TIMEOUT)
    client = TorrentClient(settings=settings)
    await client.start()

    handle = None
    try:
        # 1. 添加 torrent 并获取元数据
        # 使用 get_handle 上下文管理器确保资源被正确释放
        async with client.get_handle(ACTIVE_INFOHASH) as h:
            handle = h
            assert handle is not None, "未能获取 torrent handle"
            assert handle.is_valid(), "获取的 torrent handle 无效"

            ti = await client._execute_sync(handle.get_torrent_info)
            assert ti is not None, "元数据下载失败 (torrent_info 为 None)"
            assert ti.num_pieces() > 0, "元数据中的 piece 数量无效"

            # 2. 下载并读取第一个 piece
            # piece 0 通常是元数据或文件头部，比较容易获取
            pieces_data = await client.fetch_pieces(handle, [0], timeout=PIECE_FETCH_TIMEOUT)

            assert isinstance(pieces_data, dict), "fetch_pieces 未返回字典"
            assert 0 in pieces_data, "返回的数据中不包含 piece 0"
            assert isinstance(pieces_data[0], bytes), "piece 0 的数据不是 bytes 类型"
            assert len(pieces_data[0]) > 0, "piece 0 的数据为空"

            # 验证 piece 的长度是否与元数据中定义的一致
            assert len(pieces_data[0]) == ti.piece_length(), "下载的 piece 长度与预期不符"

    except MetadataTimeoutError:
        pytest.fail(f"获取 infohash {ACTIVE_INFOHASH} 的元数据超时，测试失败。请检查网络连接或更换 infohash。")
    except asyncio.TimeoutError:
        pytest.fail(f"下载 piece 0 超时，测试失败。")
    finally:
        # 确保客户端被停止
        await client.stop()
