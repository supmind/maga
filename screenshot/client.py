# -*- coding: utf-8 -*-
"""
本模块包含 TorrentClient 类，它负责所有与 libtorrent 库的直接交互。
其设计目标是为上层业务（如 ScreenshotService）提供一个简洁、异步的接口，
同时封装 libtorrent 的复杂性。

重构后的架构 (V2 - 线程安全模式):
- TorrentClient: 仍然是主入口点。
- _TorrentRequest: 封装与单个 torrent 相关的状态。
- _alert_loop: 现在委托给一个在线程池中运行的同步方法。
- _sync_alert_fetcher: 此方法在执行器线程中运行。它执行阻塞的 libtorrent 调用
  (wait_for_alert, pop_alerts)，然后使用 `call_soon_threadsafe` 将收到的警报
  安全地调度回主事件循环进行处理。
- _process_alerts: 此方法在主事件循环中运行，负责处理警报并更新 asyncio 对象
  (Events, Queues)，从而避免了死锁。
"""
import asyncio
import logging
import os
import time
import libtorrent as lt
from collections import OrderedDict
from typing import Dict, Set, List

class LibtorrentError(Exception):
    """自定义异常，用于清晰地传递来自 libtorrent 核心的特定错误。"""
    def __init__(self, error_code_or_message):
        if hasattr(error_code_or_message, 'message'):
            message = error_code_or_message.message()
        else:
            message = str(error_code_or_message)
        super().__init__(f"Libtorrent 错误: {message}")


class _TorrentRequest:
    """封装与单个 torrent 相关的所有状态和异步原语。"""
    def __init__(self, handle, loop):
        self.handle = handle
        self.loop = loop
        self.log = logging.getLogger(f"TorrentRequest.{str(handle.info_hash())[:6]}")
        self.log.debug("请求已创建。")
        self.metadata_event = asyncio.Event()
        self.piece_data_cache: Dict[int, bytes] = {}
        self.piece_read_events: Dict[int, asyncio.Event] = {}
        self.piece_fetch_queues: Set[asyncio.Queue] = set()

    def _get_read_event(self, piece_index: int) -> asyncio.Event:
        if piece_index not in self.piece_read_events:
            self.piece_read_events[piece_index] = asyncio.Event()
        return self.piece_read_events[piece_index]

    async def read_piece(self, piece_index: int) -> bytes:
        if piece_index in self.piece_data_cache:
            return self.piece_data_cache[piece_index]
        event = self._get_read_event(piece_index)
        await self.loop.run_in_executor(None, self.handle.read_piece, piece_index)
        await event.wait()
        return self.piece_data_cache[piece_index]

    def handle_metadata_received(self):
        self.log.info("元数据已接收，正在设置事件。")
        self.metadata_event.set()

    def handle_piece_finished(self, alert):
        piece_index = alert.piece_index
        for queue in self.piece_fetch_queues:
            queue.put_nowait(piece_index)

    def handle_read_piece(self, alert):
        piece_index = alert.piece
        event = self._get_read_event(piece_index)
        if alert.error and alert.error.value() != 0:
            self.log.error(f"读取 piece {piece_index} 时出错: {alert.error.message()}")
        else:
            self.piece_data_cache[piece_index] = bytes(alert.buffer)
            event.set()

class TorrentClient:
    """一个 libtorrent 会话的包装器，用于处理 torrent 相关操作。"""
    def __init__(self, loop=None, save_path='/dev/shm', max_cache_size=50):
        self.loop = loop or asyncio.get_event_loop()
        self.log = logging.getLogger("TorrentClient")
        self.save_path = save_path
        self.max_cache_size = max_cache_size
        self.ses = lt.session({
            'listen_interfaces': '0.0.0.0:6881',
            'enable_dht': True,
            'alert_mask': (lt.alert_category.error | lt.alert_category.status | lt.alert_category.storage | lt.alert_category.piece_progress),
            'dht_bootstrap_nodes': 'dht.libtorrent.org:25401,router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881,router.bt.ouinet.work:6881',
            'user_agent': 'qBittorrent/4.5.2', 'peer_fingerprint': 'qB4520',
        })
        self.trackers = [] # Tracker 列表为空以简化
        self._running = False
        self.alert_task = None
        self.requests: Dict[str, _TorrentRequest] = OrderedDict()
        self.finished_piece_queue = asyncio.Queue()

    async def start(self):
        self.log.info("正在启动 TorrentClient...")
        self._running = True
        self.alert_task = self.loop.create_task(self._alert_loop())
        self.log.info("TorrentClient 已启动。")

    def stop(self):
        self.log.info("正在停止 TorrentClient...")
        self._running = False
        if self.alert_task: self.alert_task.cancel()
        self.log.info("TorrentClient 已停止。")

    async def add_torrent(self, infohash: str, timeout: int = 180):
        if infohash in self.requests:
            req = self.requests[infohash]
            is_valid = await self.loop.run_in_executor(None, lambda: req.handle.is_valid())
            if is_valid:
                self.log.info(f"从缓存返回 {infohash} 的现有句柄。")
                self.requests.move_to_end(infohash)
                return req.handle

        if len(self.requests) >= self.max_cache_size:
            old_infohash, old_req = self.requests.popitem(last=False)
            await self.remove_torrent(old_req.handle)

        def _add_torrent_sync():
            params = lt.parse_magnet_uri(f"magnet:?xt=urn:btih:{infohash}")
            params.save_path = os.path.join(self.save_path, infohash)
            return self.ses.add_torrent(params)

        handle = await self.loop.run_in_executor(None, _add_torrent_sync)
        req = _TorrentRequest(handle, self.loop)
        self.requests[infohash] = req

        try:
            await asyncio.wait_for(req.metadata_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            self.log.error(f"为 {infohash} 获取元数据超时。")
            self.requests.pop(infohash, None)
            await self.loop.run_in_executor(None, self.ses.remove_torrent, handle, lt.session.delete_files)
            raise LibtorrentError(f"为 {infohash} 获取元数据超时。")

        def _pause_and_set_priorities_sync(h):
            h.pause()
            ti = h.get_torrent_info()
            if ti: h.piece_priority([i for i in range(ti.num_pieces())], 0)

        await self.loop.run_in_executor(None, _pause_and_set_priorities_sync, handle)
        return handle

    async def remove_torrent(self, handle):
        is_valid = await self.loop.run_in_executor(None, lambda: handle and handle.is_valid())
        if not is_valid: return
        infohash = str(handle.info_hash())
        self.requests.pop(infohash, None)
        await self.loop.run_in_executor(None, self.ses.remove_torrent, handle, lt.session.delete_files)

    async def request_pieces(self, handle, piece_indices: list[int]):
        is_valid = await self.loop.run_in_executor(None, lambda: handle.is_valid())
        if not is_valid or not piece_indices: return

        def _request_sync():
            unique_indices = sorted(list(set(p for p in piece_indices if not handle.have_piece(p))))
            if unique_indices:
                handle.piece_priority(unique_indices, 7)
                handle.resume()
        await self.loop.run_in_executor(None, _request_sync)

    async def fetch_pieces(self, handle, piece_indices: list[int], timeout=300.0) -> dict[int, bytes]:
        is_valid = await self.loop.run_in_executor(None, lambda: handle.is_valid())
        if not is_valid: raise LibtorrentError("获取 pieces 时使用了无效的句柄。")
        if not piece_indices: return {}

        req = self.requests.get(str(handle.info_hash()))
        if not req: raise LibtorrentError("找不到与句柄关联的 Torrent 请求。")

        unique_indices = sorted(list(set(piece_indices)))

        async def _fetch_logic():
            def _get_needed_sync():
                return [p for p in unique_indices if not handle.have_piece(p)]
            needed_pieces = set(await self.loop.run_in_executor(None, _get_needed_sync))

            fetch_queue = None
            if needed_pieces:
                fetch_queue = asyncio.Queue()
                req.piece_fetch_queues.add(fetch_queue)
                await self.request_pieces(handle, list(needed_pieces))

            try:
                while needed_pieces:
                    finished_piece = await fetch_queue.get()
                    needed_pieces.discard(finished_piece)
                    fetch_queue.task_done()
            finally:
                if fetch_queue:
                    req.piece_fetch_queues.discard(fetch_queue)

            read_tasks = [req.read_piece(p) for p in unique_indices]
            results = await asyncio.gather(*read_tasks)
            return dict(zip(unique_indices, results))

        try:
            return await asyncio.wait_for(_fetch_logic(), timeout=timeout)
        except asyncio.TimeoutError:
            raise LibtorrentError(f"下载或读取 pieces {unique_indices} 超时。")

    def _process_alerts(self, alerts: List[lt.alert]):
        """在事件循环中同步处理警报。"""
        for alert in alerts:
            handle = getattr(alert, 'handle', None)
            if not handle or not handle.is_valid(): continue

            infohash = str(handle.info_hash())
            req = self.requests.get(infohash)
            if not req: continue

            alert_type = type(alert)
            if alert_type == lt.metadata_received_alert:
                req.handle_metadata_received()
            elif alert_type == lt.piece_finished_alert:
                self.finished_piece_queue.put_nowait(alert.piece_index)
                req.handle_piece_finished(alert)
            elif alert_type == lt.read_piece_alert:
                req.handle_read_piece(alert)

    def _sync_alert_fetcher(self):
        """在执行器线程中运行的阻塞函数。"""
        self.ses.wait_for_alert(500)
        alerts = self.ses.pop_alerts()
        if alerts:
            # 将警报的处理调度回主事件循环
            self.loop.call_soon_threadsafe(self._process_alerts, alerts)

    async def _alert_loop(self):
        """异步循环，将阻塞工作委托给执行器。"""
        while self._running:
            try:
                await self.loop.run_in_executor(None, self._sync_alert_fetcher)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("ALERT_LOOP: 发生未处理的错误。")
                await asyncio.sleep(1)
