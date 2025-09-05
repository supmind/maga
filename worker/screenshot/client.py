# -*- coding: utf-8 -*-
"""
该模块包含 TorrentClient 类，它是一个围绕 libtorrent 库的异步包装器，
负责所有与 BitTorrent 网络的直接交互。

核心设计理念：
1.  **异步/同步桥接**: libtorrent 是一个同步的、基于回调的库。为了将其集成到 asyncio 应用中，
    本客户端在一个独立的后台线程中运行 libtorrent 的主事件循环 (`_alert_loop`)。
2.  **线程安全**: 所有对 libtorrent 会话对象 (`self._ses`) 的访问都被 channeled 到
    这个专用的后台线程中，通过一个生产者-消费者队列 (`self._cmd_queue`) 来实现，
    避免了使用锁。
3.  **Future 驱动的 API**: 将 libtorrent 的回调事件（如元数据接收、数据块完成）
    转换为 `asyncio.Future` 对象，使得上层调用者可以使用 `await` 语法来等待这些事件。
"""
import asyncio
import logging
import os
import time
import libtorrent as lt
import threading
import queue
from collections import defaultdict
from contextlib import asynccontextmanager
import tempfile

from .errors import TorrentClientError, MetadataTimeoutError
from config import Settings


class TorrentClient:
    """
    一个 libtorrent 会话的异步包装器，用于处理 torrent 相关操作。
    """
    def __init__(self, loop=None, settings: Settings = None):
        self.loop = loop or asyncio.get_event_loop()
        self.log = logging.getLogger("TorrentClient")

        # 如果没有提供 settings 对象，则使用默认值创建一个
        app_settings = settings or Settings()

        self.save_path = app_settings.torrent_save_path
        self.metadata_timeout = app_settings.metadata_timeout

        # libtorrent 会话设置，从应用配置中读取
        settings_pack = {
            # 基本设置
            'listen_interfaces': app_settings.lt_listen_interfaces,
            'user_agent': 'qBittorrent/4.5.2',
            'peer_fingerprint': 'qB4520',
            'dht_bootstrap_nodes': 'dht.libtorrent.org:25401,router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881,router.bt.ouinet.work:6881',
            'enable_dht': True,

            # 性能调优设置
            'active_limit': app_settings.lt_active_limit,
            'active_downloads': app_settings.lt_active_downloads,
            'connections_limit': app_settings.lt_connections_limit,
            'upload_rate_limit': app_settings.lt_upload_rate_limit,
            'download_rate_limit': app_settings.lt_download_rate_limit,
            'peer_connect_timeout': app_settings.lt_peer_connect_timeout,

            # 缓存设置 (单位: 16KiB 块)
            'cache_size': app_settings.lt_cache_size,

            # 警报掩码
            'alert_mask': (
                lt.alert_category.error |
                lt.alert_category.status |
                lt.alert_category.storage |
                lt.alert_category.piece_progress
            ),
        }
        # 所有对会话的调用都必须在专用的 libtorrent 线程中完成。
        # 我们使用一个队列来从 asyncio 线程封送调用。
        self._ses = lt.session(settings_pack)
        self._cmd_queue = queue.Queue()

        self._thread = None
        self._running = False

        self.dht_ready = asyncio.Event()
        # 用于跟踪等待元数据下载的 Future: {infohash: Future}
        self.pending_metadata = {}
        # 用于跟踪正在进行的 piece 读取请求: {(infohash, piece_idx): Future}
        self.pending_reads = {}
        self.pending_reads_lock = threading.Lock()

        # 用于跟踪 `fetch_pieces` 的批量请求: {fetch_id: {future, remaining_pieces, infohash}}
        self.pending_fetches = {}
        self.fetch_lock = threading.Lock()
        self.next_fetch_id = 0

        self.last_dht_log_time = 0

        # 用于 piece 完成事件的发布/订阅系统: {infohash: [Queue, ...]}
        self.piece_subscribers = defaultdict(list)
        self.subscribers_lock = threading.Lock()

    async def _execute_sync(self, func, *args, **kwargs):
        """
        在 libtorrent 线程上异步执行一个函数，并等待其结果。
        这是与 libtorrent 会话交互的主要方式。
        """
        future = self.loop.create_future()
        self._cmd_queue.put((future, func, args, kwargs))
        # 使用一个线程安全的调用来唤醒会话的 wait_for_alert()，
        # 以便它可以立即处理我们的命令。
        self._ses.post_session_stats()
        return await future

    def _execute_sync_nowait(self, func, *args, **kwargs):
        """以“即发即忘”的方式在 libtorrent 线程上执行一个函数。"""
        self._cmd_queue.put((None, func, args, kwargs))
        self._ses.post_session_stats()

    async def start(self):
        """启动 torrent 客户端并开始在后台线程中监听警报。"""
        self.log.info("正在启动 TorrentClient...")
        self._running = True
        self._thread = threading.Thread(target=self._alert_loop, daemon=True)
        self._thread.start()
        self.log.info("TorrentClient 已启动。")

    async def stop(self):
        """异步地、优雅地停止 torrent 客户端。"""
        self.log.info("正在停止 TorrentClient...")
        self._running = False
        if self._thread and self._thread.is_alive():
            # 发送一个空操作警报以立即唤醒 `wait_for_alert()` 调用，使其能检查 `self._running` 标志。
            self._ses.post_dht_stats()
            # 将阻塞的 `join` 操作放入执行器中，以避免阻塞事件循环。
            await self.loop.run_in_executor(None, self._thread.join)
        self.log.info("TorrentClient 已停止。")

    def subscribe_pieces(self, infohash: str, queue: asyncio.Queue):
        """
        订阅一个特定 infohash 的 piece 完成事件。
        当该 infohash 的任何 piece 下载完成时，其索引将被放入提供的队列中。
        """
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
        """
        一个异步上下文管理器，用于安全地获取和释放 torrent handle。
        推荐使用 `async with` 语句来调用此方法，以确保资源被正确清理。
        """
        handle = None
        try:
            handle = await self.add_torrent(infohash, metadata=metadata)
            if not handle or not handle.is_valid():
                raise TorrentClientError(f"无法为 {infohash} 获取有效的 torrent handle。")
            yield handle
        finally:
            if handle and handle.is_valid():
                await self.remove_torrent(handle)

    async def add_torrent(self, infohash: str, metadata: bytes = None):
        """
        通过 infohash 或元数据添加 torrent。如果提供了元数据，则直接使用它。
        否则，通过磁力链接异步下载元数据。所有 piece 的优先级初始设为0（不下载）。
        """
        self.log.info("正在为 infohash 添加 torrent: %s", infohash)
        save_dir = os.path.join(self.save_path, infohash)

        if metadata:
            # --- 分支1: 直接使用提供的元数据 ---
            self.log.info("正在使用提供的元数据为 %s 添加 torrent。", infohash)
            try:
                ti = lt.torrent_info(metadata)
                if str(ti.info_hash()) != infohash:
                    raise TorrentClientError(f"提供的元数据 infohash ({ti.info_hash()}) 与指定的 infohash ({infohash}) 不匹配。")
            except RuntimeError as e:
                raise TorrentClientError(f"无法解析元数据: {e}")

            params = lt.add_torrent_params()
            params.ti = ti
            params.save_path = save_dir
            params.flags |= lt.torrent_flags.paused
            handle = await self._execute_sync(self._ses.add_torrent, params)

        else:
            # --- 分支2: 通过磁力链接获取元数据 ---
            self.log.info("没有提供元数据。正在为 %s 使用磁力链接。", infohash)
            meta_future = self.loop.create_future()
            self.pending_metadata[infohash] = meta_future

            trackers = [
                "udp://tracker.opentrackr.org:1337/announce", "udp://open.demonii.com:1337/announce",
                "udp://open.stealth.si:80/announce", "udp://exodus.desync.com:6969/announce",
                "udp://tracker.bittor.pw:1337/announce", "http://sukebei.tracker.wf:8888/announce",
                "udp://tracker.torrent.eu.org:451/announce",
            ]
            magnet_uri = f"magnet:?xt=urn:btih:{infohash}&{'&'.join(['tr=' + t for t in trackers])}"

            params = lt.parse_magnet_uri(magnet_uri)
            params.save_path = save_dir
            params.flags |= lt.torrent_flags.paused  # 以暂停状态开始，以便我们可以手动控制 piece 的下载
            handle = await self._execute_sync(self._ses.add_torrent, params)

            self.log.debug("正在等待 %s 的元数据... (超时: %ss)", infohash, self.metadata_timeout)
            try:
                handle = await asyncio.wait_for(meta_future, timeout=self.metadata_timeout)
            except asyncio.TimeoutError:
                raise MetadataTimeoutError(f"获取元数据超时", infohash=infohash)
            finally:
                self.pending_metadata.pop(infohash, None)

        # 对两种情况都设置 piece 优先级为0
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
        """
        按需请求一组特定的 piece。这是一个“即发即忘”的操作，
        它通过提升 piece 的优先级来触发下载，但不等待其完成。
        """
        if not piece_indices: return
        unique_indices = sorted(list(set(piece_indices)))
        def _request_sync():
            pieces_to_request = [p for p in unique_indices if not handle.have_piece(p)]
            if pieces_to_request:
                priorities = [(p, 7) for p in pieces_to_request]
                handle.prioritize_pieces(priorities)
                handle.resume()
        self._execute_sync_nowait(_request_sync)

    async def fetch_pieces(self, handle, piece_indices: list[int], timeout=300.0) -> dict[int, bytes]:
        """
        按需下载、读取并返回一组特定的 piece。
        这是一个阻塞操作，直到所有请求的 piece 都获取到或超时。
        """
        if not piece_indices: return {}
        infohash_hex = str(handle.info_hash())
        unique_indices = sorted(list(set(piece_indices)))
        pieces_to_download = await self._execute_sync(lambda: [p for p in unique_indices if not handle.have_piece(p)])

        if pieces_to_download:
            # --- 阶段1: 等待所有需要的 piece 下载完成 ---
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
                    'future': request_future, 'remaining': set(pieces_to_download), 'infohash': infohash_hex
                }
            try:
                await asyncio.wait_for(request_future, timeout=timeout)
            except asyncio.TimeoutError:
                raise TorrentClientError(f"下载 pieces {pieces_to_download} 超时。")
            finally:
                with self.fetch_lock: self.pending_fetches.pop(fetch_id, None)

        # --- 阶段2: 读取已下载的 piece 数据 ---
        futures_to_await, read_keys_to_await = [], []
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
            raise TorrentClientError(f"读取 pieces {unique_indices} 超时或失败: {e}") from e
        except asyncio.CancelledError:
            with self.pending_reads_lock:
                for i, f in enumerate(futures_to_await):
                    f.cancel()
                    self.pending_reads.pop(read_keys_to_await[i], None)
            raise

    def _handle_metadata_received(self, alert):
        """警报处理：元数据已收到。唤醒等待的 Future。"""
        infohash_str = str(alert.handle.info_hash())
        future = self.pending_metadata.get(infohash_str)
        if future and not future.done():
            self.loop.call_soon_threadsafe(future.set_result, alert.handle)

    def _handle_piece_finished(self, alert):
        """警报处理：一个 piece 已完成。通知 `fetch_pieces` 和订阅者。"""
        piece_index, infohash_hex = alert.piece_index, str(alert.handle.info_hash())

        # 检查是否有 `fetch_pieces` 请求在等待这个 piece
        with self.fetch_lock:
            for fetch_id, request in list(self.pending_fetches.items()):
                if request['infohash'] == infohash_hex and piece_index in request['remaining']:
                    request['remaining'].remove(piece_index)
                    if not request['remaining']:
                        future = request['future']
                        if not future.done():
                            self.loop.call_soon_threadsafe(future.set_result, True)
                        self.pending_fetches.pop(fetch_id, None)

        # 通知所有订阅了此 infohash 的队列
        with self.subscribers_lock:
            if infohash_hex in self.piece_subscribers:
                for queue in self.piece_subscribers[infohash_hex]:
                    self.loop.call_soon_threadsafe(queue.put_nowait, piece_index)

    def _handle_dht_bootstrap(self, alert):
        """警报处理：DHT 网络已成功引导。"""
        if not self.dht_ready.is_set():
            self.loop.call_soon_threadsafe(self.dht_ready.set)

    def _handle_torrent_finished(self, alert):
        """警报处理：整个 torrent 下载完成。"""
        infohash_hex = str(alert.handle.info_hash())

        # 标记所有相关的 `fetch_pieces` 请求为完成
        with self.fetch_lock:
            for fetch_id, request in list(self.pending_fetches.items()):
                if request['infohash'] == infohash_hex:
                    future = request['future']
                    if not future.done():
                        self.loop.call_soon_threadsafe(future.set_result, True)
                    self.pending_fetches.pop(fetch_id, None)

        # 向订阅者发送完成信号 (None)
        with self.subscribers_lock:
            if infohash_hex in self.piece_subscribers:
                for queue in self.piece_subscribers[infohash_hex]:
                    self.loop.call_soon_threadsafe(queue.put_nowait, None)

    async def _retry_read_piece(self, read_key, handle):
        """在一个短暂的延迟后重试读取 piece。"""
        await asyncio.sleep(0.2)
        infohash_hex, piece_index = read_key
        self._execute_sync_nowait(handle.read_piece, piece_index)

    def _handle_read_piece(self, alert):
        """警报处理：读取 piece 的操作已完成 (成功或失败)。"""
        infohash_hex, piece_index = str(alert.handle.info_hash()), alert.piece
        read_key = (infohash_hex, piece_index)

        with self.pending_reads_lock:
            pending_info = self.pending_reads.get(read_key)

        if not pending_info or pending_info['future'].done(): return

        error = None
        if alert.error and alert.error.value() != 0:
            error_message = alert.error.message()
            # 对特定的、可能是暂时性的错误进行重试
            if "invalid piece index" in error_message and pending_info['retries'] < 5:
                pending_info['retries'] += 1
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
        """
        在后台线程中运行的主循环。它持续等待并处理来自 libtorrent 的警报，
        并执行来自 asyncio 事件循环的命令。
        """
        while self._running:
            # --- 阶段 1: 处理来自 asyncio 事件循环的命令 ---
            while not self._cmd_queue.empty():
                try:
                    future, func, args, kwargs = self._cmd_queue.get_nowait()
                    try:
                        result = func(*args, **kwargs)
                        if future and not future.done():
                            self.loop.call_soon_threadsafe(future.set_result, result)
                    except Exception as e:
                        if future and not future.done():
                            self.loop.call_soon_threadsafe(future.set_exception, e)
                except queue.Empty:
                    break  # 即使在检查后队列也可能变空

            # --- 阶段 2: 处理 libtorrent 警报 ---
            if not self._running:
                break

            # 等待最多200毫秒。这决定了循环对 `self._running` 标志变化的响应性。
            # 实际的命令响应由 `_ses.post_session_stats()` 触发的即时唤醒处理。
            alert = self._ses.wait_for_alert(200)
            if not alert:
                continue

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

                # 打印状态更新日志
                elif type(alert) == lt.state_update_alert:
                    for s in alert.status:
                        if s.state != lt.torrent_status.states.seeding:
                            self.log.info(
                                "  状态更新 - %s: %s %.2f%% | 下载速度: %.1f kB/s | 节点: %d (%d 种子)",
                                s.name, s.state_str, s.progress * 100,
                                s.download_rate / 1000, s.num_peers, s.num_seeds
                            )
            # 定期打印 DHT 状态
            now = time.time()
            if now - self.last_dht_log_time > 10:
                status = self._ses.status()
                self.log.info("DHT 状态: %d 个节点。", status.dht_nodes)
                self.last_dht_log_time = now
