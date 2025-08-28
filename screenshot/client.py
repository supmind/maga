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
from concurrent.futures import Future

class LibtorrentError(Exception):
    """自定义异常，用于清晰地传递来自 libtorrent 核心的特定错误。"""
    def __init__(self, error_code_or_message):
        if hasattr(error_code_or_message, 'message'):
            # It's a libtorrent error code object
            self.error_code = error_code_or_message
            message = self.error_code.message()
        else:
            # It's a simple string message
            self.error_code = None
            message = str(error_code_or_message)
        super().__init__(f"Libtorrent 错误: {message}")

class TorrentClient:
    """一个 libtorrent 会话的包装器，用于处理 torrent 相关操作。"""
    def __init__(self, loop=None, save_path='/dev/shm'):
        self.loop = loop or asyncio.get_event_loop()
        self.log = logging.getLogger("TorrentClient")
        self.save_path = save_path
        self.ses = lt.session()
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
        self.ses.apply_settings(settings)
        self.alert_task = None
        self._running = False
        self.dht_ready = asyncio.Event()
        self.pending_metadata = {}
        self.pending_reads = defaultdict(list)
        self.pending_reads_lock = threading.Lock()
        self.pending_fetches = {}
        self.fetch_lock = threading.Lock()
        self.next_fetch_id = 0
        self.last_dht_log_time = 0

    async def start(self):
        """启动 torrent 客户端的警报循环。"""
        self.log.info("正在启动 TorrentClient...")
        self._running = True
        self.alert_task = self.loop.create_task(self._alert_loop())
        self.log.info("TorrentClient 已启动。")

    def stop(self):
        """停止 torrent 客户端的警报循环。"""
        self.log.info("正在停止 TorrentClient...")
        self._running = False
        if self.alert_task:
            self.alert_task.cancel()
        self.log.info("TorrentClient 已停止。")

    async def add_torrent(self, infohash: str):
        """通过 infohash 添加 torrent，并在收到元数据后返回句柄。"""
        self.log.info(f"正在为 infohash 添加 torrent: {infohash}")
        save_dir = os.path.join(self.save_path, infohash)

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
        # Add the torrent in a paused state so we can set piece priorities before downloading.
        params.flags |= lt.torrent_flags.paused
        handle = self.ses.add_torrent(params)

        self.log.debug("Waiting for DHT bootstrap...")
        try:
            await asyncio.wait_for(self.dht_ready.wait(), timeout=30)
            self.log.info("DHT bootstrap successful.")
        except asyncio.TimeoutError:
            self.log.warning("DHT bootstrap timed out, proceeding without it.")

        self.log.debug(f"正在等待 {infohash} 的元数据...")
        try:
            handle = await asyncio.wait_for(meta_future, timeout=180)
        except asyncio.TimeoutError:
            self.log.error(f"为 {infohash} 获取元数据超时。")
            self.pending_metadata.pop(infohash, None)
            raise LibtorrentError(f"为 {infohash} 获取元数据超时。")

        # After getting metadata, set all pieces to priority 0
        ti = handle.get_torrent_info()
        if ti:
            self.log.info(f"为 {infohash} 设置所有 piece 优先级为 0。")
            for i in range(ti.num_pieces()):
                handle.piece_priority(i, 0)

        return handle

    def remove_torrent(self, handle):
        """从会话中移除一个 torrent。"""
        if handle and handle.is_valid():
            infohash = str(handle.info_hash())
            self.pending_metadata.pop(infohash, None)
            self.ses.remove_torrent(handle, lt.session.delete_files)
            self.log.info(f"已移除 torrent: {infohash}")

    async def fetch_pieces(self, handle, piece_indices: list[int], timeout=300.0) -> dict[int, bytes]:
        """按需下载、读取并返回一组特定的 piece。"""
        if not piece_indices:
            return {}

        unique_indices = sorted(list(set(piece_indices)))
        pieces_to_download = [p for p in unique_indices if not handle.have_piece(p)]

        if pieces_to_download:
            self.log.info(f"需要下载 {len(pieces_to_download)} 个 pieces: {pieces_to_download}")
            # Set priorities for the pieces we want to download.
            self.log.info(f"为 pieces {pieces_to_download} 设置高优先级。")
            for p in pieces_to_download:
                handle.piece_priority(p, 7)  # 7 is the highest priority
            handle.resume()

            # Create a future to wait for all pieces in this batch to download
            request_future = self.loop.create_future()
            with self.fetch_lock:
                fetch_id = self.next_fetch_id
                self.next_fetch_id += 1
                self.pending_fetches[fetch_id] = {
                    'future': request_future,
                    'remaining': set(pieces_to_download)
                }

            try:
                self.log.info(f"开始等待 pieces 下载: {pieces_to_download}")
                await asyncio.wait_for(request_future, timeout=timeout)
                self.log.info(f"成功等到 pieces: {pieces_to_download}")
            except asyncio.TimeoutError:
                self.log.error(f"等待 pieces {pieces_to_download} 超时。")
                with self.fetch_lock: self.pending_fetches.pop(fetch_id, None)
                raise LibtorrentError(f"下载 pieces {pieces_to_download} 超时。")

        self.log.info(f"所有需要的 pieces ({unique_indices}) 均已就绪，开始读取。")

        async def _read_piece(piece_index):
            read_future = self.loop.create_future()
            with self.pending_reads_lock:
                self.pending_reads[piece_index].append(read_future)
            handle.read_piece(piece_index)
            return await read_future

        try:
            tasks = [_read_piece(p) for p in unique_indices]
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
            return dict(zip(unique_indices, results))
        except asyncio.TimeoutError:
            raise LibtorrentError(f"读取 pieces {unique_indices} 超时。")


    def _handle_metadata_received(self, alert):
        """处理接收到元数据的警报。"""
        infohash_str = str(alert.handle.info_hash())
        if infohash_str in self.pending_metadata and not self.pending_metadata[infohash_str].done():
            self.pending_metadata[infohash_str].set_result(alert.handle)

    def _handle_piece_finished(self, alert):
        """处理 piece 下载完成的警报。"""
        self.log.info(f"收到 piece 下载完成通知: piece #{alert.piece_index}")
        piece_index = alert.piece_index
        with self.fetch_lock:
            # Using list() to avoid issues with modifying dict during iteration
            for fetch_id, request in list(self.pending_fetches.items()):
                if piece_index in request['remaining']:
                    request['remaining'].remove(piece_index)
                    if not request['remaining']:
                        request['future'].set_result(True)
                        self.pending_fetches.pop(fetch_id, None)

    def _handle_dht_bootstrap(self, alert):
        """处理 DHT 引导完成的警报。"""
        if not self.dht_ready.is_set(): self.dht_ready.set()

    def _handle_read_piece(self, alert):
        """处理 piece 读取完成的警报。"""
        with self.pending_reads_lock:
            futures = self.pending_reads.pop(alert.piece, [])

        error = None
        if alert.error and alert.error.value() != 0:
            if "success" not in alert.error.message().lower():
                error = LibtorrentError(alert.error)

        data = bytes(alert.buffer)
        for future in futures:
            if not future.done():
                if error:
                    future.set_exception(error)
                else:
                    future.set_result(data)

    async def _alert_loop(self):
        """libtorrent 会话的主警报处理循环。"""
        while self._running:
            try:
                alerts = self.ses.pop_alerts()
                for alert in alerts:
                    if alert.category() & lt.alert_category.error:
                        self.log.error(f"Libtorrent 警报: {alert}")
                    else:
                        self.log.debug(f"Libtorrent 警报: {alert}")

                    alert_type = type(alert)
                    if alert_type == lt.metadata_received_alert:
                        self._handle_metadata_received(alert)
                    elif alert_type == lt.piece_finished_alert:
                        self._handle_piece_finished(alert)
                    elif alert_type == lt.read_piece_alert:
                        self._handle_read_piece(alert)
                    elif alert_type == lt.dht_bootstrap_alert:
                        self._handle_dht_bootstrap(alert)
                    elif alert_type == lt.state_update_alert:
                        for s in alert.status:
                            if s.state != lt.torrent_status.states.seeding:
                                self.log.info(
                                    f"  状态更新 - {s.name}: {s.state_str} {s.progress * 100:.2f}% "
                                    f"| 下载速度: {s.download_rate / 1000:.1f} kB/s "
                                    f"| 节点: {s.num_peers} ({s.num_seeds} 种子)"
                                )

                now = time.time()
                if now - self.last_dht_log_time > 10:
                    status = self.ses.status()
                    self.log.info(f"DHT 状态: {status.dht_nodes} 个节点。")
                    self.last_dht_log_time = now

                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("libtorrent 警报循环中发生错误。")
