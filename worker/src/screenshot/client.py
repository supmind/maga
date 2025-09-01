# -*- coding: utf-8 -*-
"""
该模块包含 TorrentClient 类，它负责所有与 libtorrent 库的直接交互。
"""
import asyncio
import logging
import os
import time
import libtorrent as lt
import threading
from collections import defaultdict
from contextlib import asynccontextmanager

from .errors import TorrentClientError, MetadataTimeoutError


import tempfile


class TorrentClient:
    """一个 libtorrent 会话的包装器，用于处理 torrent 相关操作。"""
    def __init__(self, loop=None, save_path: str = None, metadata_timeout: int = 180):
        self.loop = loop or asyncio.get_event_loop()
        self.log = logging.getLogger("TorrentClient")
        # 兼容性修复：如果未提供 save_path，则使用系统通用的临时目录
        self.save_path = save_path or os.path.join(tempfile.gettempdir(), 'screenshot_service_torrents')
        self.metadata_timeout = metadata_timeout

        settings = {
            'listen_interfaces': '0.0.0.0:6881',
            'enable_dht': True,
            'alert_mask': (
                lt.alert_category.error |
                lt.alert_category.status |
                lt.alert_category.storage |
                lt.alert_category.piece_progress
            ),
            'dht_bootstrap_nodes': 'dht.libtorrent.org:25401,router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881,router.bt.ouinet.work:6881',
            'user_agent': 'qBittorrent/4.5.2',
            'peer_fingerprint': 'qB4520',
        }
        # 会话及其对象不是线程安全的，需要锁来保护。
        self._ses = lt.session(settings)
        self._ses_lock = threading.Lock()

        self._thread = None
        self._running = False

        self.dht_ready = asyncio.Event()
        # 用于跟踪等待元数据下载的 future
        self.pending_metadata = {}
        # 用于跟踪正在进行的 piece 读取请求
        self.pending_reads = {}
        self.pending_reads_lock = threading.Lock()

        # 用于跟踪 `fetch_pieces` 的请求
        self.pending_fetches = {}
        self.fetch_lock = threading.Lock()
        self.next_fetch_id = 0

        self.last_dht_log_time = 0

        # --- 用于 piece 完成事件的发布/订阅系统 ---
        self.piece_subscribers = defaultdict(list)
        self.subscribers_lock = threading.Lock()

    async def _execute_sync(self, func, *args, **kwargs):
        """在一个独立的线程中执行同步的 libtorrent 调用，以避免阻塞事件循环。"""
        return await self.loop.run_in_executor(None, self._sync_wrapper, func, *args, **kwargs)

    def _sync_wrapper(self, func, *args, **kwargs):
        """在执行 libtorrent 调用前获取会话锁。"""
        with self._ses_lock:
            return func(*args, **kwargs)

    def _execute_sync_nowait(self, func, *args, **kwargs):
        """执行一个 libtorrent 调用，但不等待其完成。"""
        self.loop.run_in_executor(None, self._sync_wrapper, func, *args, **kwargs)

    async def start(self):
        """启动 torrent 客户端并开始监听警报。"""
        self.log.info("正在启动 TorrentClient...")
        self._running = True
        self._thread = threading.Thread(target=self._alert_loop, daemon=True)
        self._thread.start()
        self.log.info("TorrentClient 已启动。")

    async def stop(self):
        """异步地停止 torrent 客户端。"""
        self.log.info("正在停止 TorrentClient...")
        self._running = False
        if self._thread and self._thread.is_alive():
            # 发送一个空操作警报以唤醒 wait_for_alert() 调用
            self._execute_sync_nowait(self._ses.post_dht_stats)
            # 将阻塞的 join 操作放入执行器中，以避免阻塞事件循环
            await self.loop.run_in_executor(None, self._thread.join)
        self.log.info("TorrentClient 已停止。")

    def subscribe_pieces(self, infohash: str, queue: asyncio.Queue):
        """订阅一个特定 infohash 的 piece 完成事件。"""
        with self.subscribers_lock:
            self.log.debug("[%s] 新增一个订阅者，当前订阅者数量: %d", infohash, len(self.piece_subscribers[infohash]) + 1)
            self.piece_subscribers[infohash].append(queue)

    def unsubscribe_pieces(self, infohash: str, queue: asyncio.Queue):
        """取消订阅一个特定 infohash 的 piece 完成事件。"""
        with self.subscribers_lock:
            self.log.debug("[%s] 移除一个订阅者...", infohash)
            try:
                self.piece_subscribers[infohash].remove(queue)
                if not self.piece_subscribers[infohash]:
                    del self.piece_subscribers[infohash]
                self.log.debug("[%s] 订阅者移除成功。", infohash)
            except ValueError:
                self.log.warning("[%s] 尝试移除一个不存在的订阅者。", infohash)

    @asynccontextmanager
    async def get_handle(self, infohash: str, metadata: bytes = None):
        """一个异步上下文管理器，用于安全地获取和释放 torrent handle。"""
        handle = None
        try:
            # 调用现有的 add_torrent 来获取 handle
            handle = await self.add_torrent(infohash, metadata=metadata)
            if not handle or not handle.is_valid():
                # 如果 handle 无效，我们在这里就引发异常，确保 finally 块不会尝试移除无效的 handle
                raise TorrentClientError("无法为 %s 获取有效的 torrent handle。", infohash)

            # 将有效的 handle 交给 'async with' 代码块
            yield handle
        finally:
            # 当 'async with' 代码块结束时（无论是否发生异常），这个部分都会执行
            if handle and handle.is_valid():
                await self.remove_torrent(handle)

    async def add_torrent(self, infohash: str, metadata: bytes = None):
        """通过 infohash 或元数据添加 torrent。如果提供了元数据，则直接使用它。否则，通过磁力链接下载。"""
        self.log.info("正在为 infohash 添加 torrent: %s", infohash)
        save_dir = os.path.join(self.save_path, infohash)

        if metadata:
            self.log.info("正在使用提供的元数据为 %s 添加 torrent。", infohash)
            try:
                ti = lt.torrent_info(metadata)
                if str(ti.info_hash()) != infohash:
                    self.log.error("提供的元数据 infohash (%s) 与指定的 infohash (%s) 不匹配。", str(ti.info_hash()), infohash)
                    raise TorrentClientError(f"提供的元数据 infohash ({ti.info_hash()}) 与指定的 infohash ({infohash}) 不匹配。")
            except RuntimeError as e:
                self.log.error("解析提供的元数据时出错: %s", e)
                raise TorrentClientError(f"无法解析元数据: {e}")

            params = lt.add_torrent_params()
            params.ti = ti
            params.save_path = save_dir
            params.flags |= lt.torrent_flags.paused
            handle = await self._execute_sync(self._ses.add_torrent, params)

            self.log.info("为 %s 设置所有 piece 优先级为 0。", infohash)
            priorities = [0] * ti.num_pieces()
            await self._execute_sync(handle.prioritize_pieces, priorities)
            return handle

        else:
            self.log.info("没有提供元数据。正在为 %s 使用磁力链接。", infohash)
            meta_future = self.loop.create_future()
            self.pending_metadata[infohash] = meta_future

            trackers = [
                "udp://tracker.opentrackr.org:1337/announce",
                "udp://open.demonii.com:1337/announce",
                "udp://open.stealth.si:80/announce",
                "udp://exodus.desync.com:6969/announce",
                "udp://tracker.bittor.pw:1337/announce",
                "http://sukebei.tracker.wf:8888/announce",
                "udp://tracker.torrent.eu.org:451/announce",
            ]
            magnet_uri = f"magnet:?xt=urn:btih:{infohash}&{'&'.join(['tr=' + t for t in trackers])}"

            params = lt.parse_magnet_uri(magnet_uri)
            params.save_path = save_dir
            params.flags |= lt.torrent_flags.paused # 以暂停状态开始，以便我们可以手动控制 piece 的下载

            handle = await self._execute_sync(self._ses.add_torrent, params)

            self.log.debug("正在等待 %s 的元数据... (超时: %ss)", infohash, self.metadata_timeout)
            try:
                handle = await asyncio.wait_for(meta_future, timeout=self.metadata_timeout)
            except asyncio.TimeoutError:
                self.log.error("为 %s 获取元数据超时。", infohash)
                # The pop is now handled in the finally block
                raise MetadataTimeoutError(f"获取元数据超时", infohash=infohash)
            finally:
                # 无论成功、超时还是其他异常，都确保清理字典
                self.pending_metadata.pop(infohash, None)

            ti = await self._execute_sync(handle.get_torrent_info)
            if ti:
                self.log.info("为 %s 设置所有 piece 优先级为 0。", infohash)
                priorities = [0] * ti.num_pieces()
                await self._execute_sync(handle.prioritize_pieces, priorities)

            return handle

    async def remove_torrent(self, handle):
        """从会话中移除一个 torrent 并删除其文件。"""
        if handle and handle.is_valid():
            infohash = str(await self._execute_sync(handle.info_hash))
            self.pending_metadata.pop(infohash, None)
            await self._execute_sync(self._ses.remove_torrent, handle, lt.session.delete_files)
            self.log.info("已移除 torrent: %s", infohash)

    def request_pieces(self, handle, piece_indices: list[int]):
        """按需请求一组特定的 piece，这是一个“即发即忘”的操作。"""
        if not piece_indices:
            return
        unique_indices = sorted(list(set(piece_indices)))
        def _request_sync():
            pieces_to_request = [p for p in unique_indices if not handle.have_piece(p)]
            if pieces_to_request:
                self.log.info("为 pieces %s 设置高优先级。", pieces_to_request)
                priorities = [(p, 7) for p in pieces_to_request]
                handle.prioritize_pieces(priorities)
                handle.resume()
        self._execute_sync_nowait(_request_sync)

    async def fetch_pieces(self, handle, piece_indices: list[int], timeout=300.0) -> dict[int, bytes]:
        """按需下载、读取并返回一组特定的 piece。这是一个阻塞操作，直到所有 piece 都获取到或超时。"""
        if not piece_indices: return {}
        infohash_hex = str(handle.info_hash())
        unique_indices = sorted(list(set(piece_indices)))
        pieces_to_download = await self._execute_sync(lambda: [p for p in unique_indices if not handle.have_piece(p)])

        if pieces_to_download:
            self.log.info("[%s] 需要下载 %d 个 pieces: %s", infohash_hex, len(pieces_to_download), pieces_to_download)
            def _set_priorities_sync():
                priorities = [(p, 7) for p in pieces_to_download]
                handle.prioritize_pieces(priorities)
                handle.resume()
            await self._execute_sync(_set_priorities_sync)

            request_future = self.loop.create_future()
            with self.fetch_lock:
                fetch_id = self.next_fetch_id
                self.next_fetch_id += 1
                self.pending_fetches[fetch_id] = {
                    'future': request_future,
                    'remaining': set(pieces_to_download),
                    'infohash': infohash_hex
                }
                self.log.debug("[%s] 创建 fetch 请求 fetch_id=%d，等待 pieces: %s", infohash_hex, fetch_id, pieces_to_download)
            try:
                self.log.debug("[%s] fetch_id=%d 开始等待 future...", infohash_hex, fetch_id)
                await asyncio.wait_for(request_future, timeout=timeout)
                self.log.info("[%s] fetch_id=%d 成功等到 pieces: %s", infohash_hex, fetch_id, pieces_to_download)
            except asyncio.TimeoutError:
                self.log.error("[%s] fetch_id=%d 等待 pieces 超时。", infohash_hex, fetch_id)
                raise TorrentClientError("下载 pieces %s 超时。", pieces_to_download)
            finally:
                # 确保无论成功、失败或取消，都能清理待处理的 fetch 请求，防止资源泄露
                with self.fetch_lock:
                    self.pending_fetches.pop(fetch_id, None)

        self.log.info("所有需要的 pieces (%s) 均已就绪，开始读取。", unique_indices)
        futures_to_await = []
        read_keys_to_await = []
        with self.pending_reads_lock:
            for piece_index in unique_indices:
                read_key = (infohash_hex, piece_index)
                if read_key in self.pending_reads:
                    future = self.pending_reads[read_key]['future']
                else:
                    future = self.loop.create_future()
                    self.pending_reads[read_key] = {'future': future, 'retries': 0, 'handle': handle}
                    self._execute_sync_nowait(handle.read_piece, piece_index)
                futures_to_await.append(future)
                read_keys_to_await.append(read_key)
        try:
            gathered_results = await asyncio.gather(*futures_to_await)
            return dict(zip([key[1] for key in read_keys_to_await], gathered_results))
        except (TorrentClientError, asyncio.TimeoutError) as e:
            with self.pending_reads_lock:
                for key in read_keys_to_await:
                    if key in self.pending_reads and not self.pending_reads[key]['future'].done():
                         self.pending_reads.pop(key, None)
            raise TorrentClientError("读取 pieces %s 超时或失败: %s", unique_indices, e) from e
        except asyncio.CancelledError:
            with self.pending_reads_lock:
                for i, f in enumerate(futures_to_await):
                    f.cancel()
                    read_key = read_keys_to_await[i]
                    self.pending_reads.pop(read_key, None)
            raise

    def _handle_metadata_received(self, alert):
        infohash_str = str(alert.handle.info_hash())
        future = self.pending_metadata.get(infohash_str)
        if future and not future.done():
            self.loop.call_soon_threadsafe(future.set_result, alert.handle)

    def _handle_piece_finished(self, alert):
        piece_index = alert.piece_index
        infohash_hex = str(alert.handle.info_hash())
        self.log.info("[%s] 收到 piece 下载完成通知: piece #%d", infohash_hex, piece_index)

        with self.fetch_lock:
            for fetch_id, request in list(self.pending_fetches.items()):
                if request['infohash'] == infohash_hex and piece_index in request['remaining']:
                    self.log.debug("[%s] fetch_id=%d 匹配到 piece #%d。剩余: %s", infohash_hex, fetch_id, piece_index, request['remaining'])
                    request['remaining'].remove(piece_index)
                    self.log.debug("[%s] fetch_id=%d piece #%d 已移除。剩余: %s", infohash_hex, fetch_id, piece_index, request['remaining'])
                    if not request['remaining']:
                        future = request['future']
                        self.log.info("[%s] fetch_id=%d 所有 pieces 都已收到。正在解锁 future。", infohash_hex, fetch_id)
                        self.loop.call_soon_threadsafe(future.set_result, True)
                        self.pending_fetches.pop(fetch_id, None)

        with self.subscribers_lock:
            if infohash_hex in self.piece_subscribers:
                self.log.debug("[%s] 正在将 piece #%d 发布给 %d 个订阅者。", infohash_hex, piece_index, len(self.piece_subscribers[infohash_hex]))
                for queue in self.piece_subscribers[infohash_hex]:
                    self.loop.call_soon_threadsafe(queue.put_nowait, piece_index)

    def _handle_dht_bootstrap(self, alert):
        if not self.dht_ready.is_set():
            self.loop.call_soon_threadsafe(self.dht_ready.set)

    def _handle_torrent_finished(self, alert):
        infohash_hex = str(alert.handle.info_hash())
        self.log.info("[%s] Torrent 完成。正在检查待处理的请求...", infohash_hex)
        with self.fetch_lock:
            for fetch_id, request in list(self.pending_fetches.items()):
                if request['infohash'] == infohash_hex:
                    self.log.info("[%s] fetch_id=%d (fetch_pieces) 因 torrent 完成而被标记为完成。", infohash_hex, fetch_id)
                    future = request['future']
                    if not future.done():
                        self.loop.call_soon_threadsafe(future.set_result, True)
                    self.pending_fetches.pop(fetch_id, None)

        with self.subscribers_lock:
            if infohash_hex in self.piece_subscribers:
                self.log.info("[%s] 正在向 %d 个订阅者发布“完成”信号。", infohash_hex, len(self.piece_subscribers[infohash_hex]))
                for queue in self.piece_subscribers[infohash_hex]:
                    self.loop.call_soon_threadsafe(queue.put_nowait, None)

    async def _retry_read_piece(self, read_key, handle):
        await asyncio.sleep(0.2)
        infohash_hex, piece_index = read_key
        self.log.info("[%s] Retrying read for piece #%d", infohash_hex, piece_index)
        self._execute_sync_nowait(handle.read_piece, piece_index)

    def _handle_read_piece(self, alert):
        infohash_hex = str(alert.handle.info_hash())
        piece_index = alert.piece
        read_key = (infohash_hex, piece_index)

        with self.pending_reads_lock:
            pending_info = self.pending_reads.get(read_key)

        if not pending_info or pending_info['future'].done():
            return

        error = None
        if alert.error and alert.error.value() != 0:
            error_message = alert.error.message()
            if "invalid piece index" in error_message and pending_info['retries'] < 5:
                pending_info['retries'] += 1
                self.log.warning("[%s] read_piece failed for piece #%d (attempt %d). Retrying...", infohash_hex, piece_index, pending_info['retries'])
                self.loop.create_task(self._retry_read_piece(read_key, pending_info['handle']))
                return
            if "success" not in error_message.lower():
                error = TorrentClientError(error_message)

        with self.pending_reads_lock:
            self.pending_reads.pop(read_key, None)

        if error:
            self.loop.call_soon_threadsafe(pending_info['future'].set_exception, error)
        else:
            data = bytes(alert.buffer)
            self.loop.call_soon_threadsafe(pending_info['future'].set_result, data)

    def _alert_loop(self):
        while self._running:
            alerts = []
            with self._ses_lock:
                if not self._running: break
                alert = self._ses.wait_for_alert(1000)
                if alert:
                    alerts = self._ses.pop_alerts()

            for alert in alerts:
                if alert.category() & lt.alert_category.error:
                    self.log.error("Libtorrent 警报: %s", alert)
                else:
                    self.log.debug("Libtorrent 警报: %s", alert)

                alert_map = {
                    lt.metadata_received_alert: self._handle_metadata_received,
                    lt.piece_finished_alert: self._handle_piece_finished,
                    lt.read_piece_alert: self._handle_read_piece,
                    lt.dht_bootstrap_alert: self._handle_dht_bootstrap,
                    lt.torrent_finished_alert: self._handle_torrent_finished,
                }
                handler = alert_map.get(type(alert))
                if handler:
                    handler(alert)
                elif type(alert) == lt.state_update_alert:
                    with self._ses_lock:
                        for s in alert.status:
                            if s.state != lt.torrent_status.states.seeding:
                                self.log.info(
                                    "  状态更新 - %s: %s %.2f%% | 下载速度: %.1f kB/s | 节点: %d (%d 种子)",
                                    s.name, s.state_str, s.progress * 100,
                                    s.download_rate / 1000, s.num_peers, s.num_seeds
                                )
            now = time.time()
            if now - self.last_dht_log_time > 10:
                with self._ses_lock:
                    status = self._ses.status()
                    self.log.info("DHT 状态: %d 个节点。", status.dht_nodes)
                self.last_dht_log_time = now
