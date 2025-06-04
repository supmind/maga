import asyncio
import binascii
import os
import signal
import logging # Ensure logging is imported for later use if needed
# 导入wire_protocol中的get_metadata函数，用于获取torrent元数据
from wire_protocol import get_metadata

from socket import inet_ntoa
from struct import unpack

import bencoder


def proper_infohash(infohash):
    if isinstance(infohash, bytes):
        # Convert bytes to hex
        infohash = binascii.hexlify(infohash).decode('utf-8')
    return infohash.upper()


def random_node_id(size=20):
    return os.urandom(size)


def split_nodes(nodes):
    length = len(nodes)
    if (length % 26) != 0:
        return

    for i in range(0, length, 26):
        nid = nodes[i:i+20]
        ip = inet_ntoa(nodes[i+20:i+24])
        port = unpack("!H", nodes[i+24:i+26])[0]
        yield nid, ip, port


__version__ = '3.0.0'


BOOTSTRAP_NODES = (
    # BitTorrent官方路由节点
    ("router.bittorrent.com", 6881),
    # Transmission客户端使用的DHT节点
    ("dht.transmissionbt.com", 6881),
    # uTorrent客户端使用的DHT节点
    ("router.utorrent.com", 6881)
)

# Maga类是实现DHT协议的爬虫核心。
# 它通过加入DHT网络，监听其他节点发送的get_peers和announce_peer请求，
# 从而发现infohash和对应的peer地址。
# 新版本集成了wire_protocol.py中的功能，使其能够主动连接peer并尝试下载元数据。
class Maga(asyncio.DgramProtocol):
    def __init__(self, loop=None, bootstrap_nodes=BOOTSTRAP_NODES, interval=1, max_concurrent_metadata_fetches=20): # Added max_concurrent_metadata_fetches
        # 生成当前节点的ID
        self.node_id = random_node_id()
        self.transport = None
        if loop is None:
            self.loop = asyncio.new_event_loop() # 如果未提供事件循环，则创建一个新的
        else:
            self.loop = loop # 使用提供的事件循环
        self.bootstrap_nodes = bootstrap_nodes # DHT引导节点列表
        self.__running = False # 运行状态标志
        self.interval = interval # 自动查找节点的间隔时间
        # Initialize the semaphore
        self.metadata_semaphore = asyncio.Semaphore(max_concurrent_metadata_fetches)
        logging.info(f"Maga (log): Initialized with max_concurrent_metadata_fetches={max_concurrent_metadata_fetches}")

    def stop(self):
        # 停止DHT爬虫的运行
        self.__running = False # 设置运行状态为False
        self.loop.call_later(self.interval, self.loop.stop) # 在指定间隔后停止事件循环

    async def auto_find_nodes(self):
        # 自动周期性地向引导节点发送find_node请求，以发现更多节点并保持DHT网络的连接。
        self.__running = True
        while self.__running:
            await asyncio.sleep(self.interval) # 等待指定间隔
            for node in self.bootstrap_nodes: # 遍历所有引导节点
                self.find_node(addr=node) # 发送find_node请求

    def run(self, port=6881):
        # 启动DHT爬虫，监听指定端口。
        # 这是程序的主入口点。
        print(f"Maga (stdout): Starting DHT node on port {port}...") # PRINT STATEMENT
        logging.info(f"Maga (log): Starting DHT node on port {port}...")
        try:
            logging.getLogger().handlers[0].flush() # FLUSH LOGS
        except:
            pass # Ignore if no handlers or other issue
        coro = self.loop.create_datagram_endpoint(
                lambda: self, local_addr=('0.0.0.0', port) # 创建UDP端点
        )
        asyncio.set_event_loop(self.loop) # 设置当前事件循环
        transport, _ = self.loop.run_until_complete(coro) # 运行直到端点创建完成

        # 注册信号处理器，用于优雅地停止程序
        for signame in ('SIGINT', 'SIGTERM'):
            try:
                self.loop.add_signal_handler(getattr(signal, signame), self.stop)
            except NotImplementedError:
                # Windows上可能不支持某些信号
                pass

        # 向引导节点发送find_node请求以加入DHT网络
        for node in self.bootstrap_nodes:
            # Bootstrap
            self.find_node(addr=node, node_id=self.node_id)

        self.loop.create_task(self.auto_find_nodes()) # 启动自动查找节点的任务
        self.loop.run_forever() # 持续运行事件循环
        self.loop.close() # 关闭事件循环

    def datagram_received(self, data, addr):
        # 当接收到UDP数据报时由asyncio调用。
        # data: 收到的字节数据
        # addr: 发送方的地址 (ip, port)
        logging.debug(f"Maga (log): Received {len(data)} bytes from {addr}")
        try:
            logging.getLogger().handlers[0].flush() # FLUSH LOGS
        except:
            pass # Ignore if no handlers or other issue
        try:
            msg = bencoder.bdecode(data) # bdecode解码收到的数据
        except Exception as e:
            logging.warning(f"Maga (log): Failed to bdecode data from {addr}: {e}")
            return
        try:
            self.handle_message(msg, addr) # 处理解码后的消息
        except Exception as e:
            logging.error(f"Maga (log): Error handling message from {addr}: {e}", exc_info=True)
            # 发生错误，向对方发送错误信息
            # msg["t"] 是交易ID，需要包含在响应中
            if msg and b"t" in msg: # Ensure msg is not None and 't' is present
                self.send_message(data={
                    b"t": msg[b"t"],
                    b"y": b"e", # "e"表示错误类型
                    b"e": [202, b"Server Error"] # 错误码和错误信息, 确保字符串是字节串
                }, addr=addr)
            # raise e # 可以选择重新抛出异常用于调试

    def handle_message(self, msg, addr):
        # 处理解码后的DHT消息。
        # msg: 解码后的消息字典
        # addr: 发送方地址
        msg_type = msg.get(b"y", b"e") # 获取消息类型, "y"字段代表类型 ("q", "r", "e")

        if msg_type == b"e": # 错误消息
            return # 通常忽略错误消息

        if msg_type == b"r": # 响应消息
            return self.handle_response(msg, addr=addr)

        if msg_type == b'q': # 查询消息
            # 查询消息需要异步处理，因此创建一个任务
            return self.loop.create_task(
                self.handle_query(msg, addr=addr)
            )

    def handle_response(self, msg, addr):
        # 处理DHT网络中的响应(response)消息。
        # 主要用于处理find_node的响应，从中提取新的DHT节点并尝试ping它们以将其加入自己的路由表。
        args = msg.get(b"r", {}) # "r"字段包含响应数据
        if b"nodes" in args: # 如果响应中包含"nodes"信息
            for node_id, ip, port in split_nodes(args[b"nodes"]): # 解析节点信息
                # Only ping if the node is not one of our bootstrap nodes
                node_addr = (ip, port)
                # self.bootstrap_nodes is a list of (host, port) tuples.
                # Some hosts might be domain names. We should compare resolved IPs if possible,
                # but for simplicity in this context, we'll compare directly if the bootstrap_nodes
                # were defined with IPs, or rely on direct tuple comparison.
                # A more robust check would involve resolving bootstrap hostnames to IPs once
                # and storing them for comparison, but that's beyond this minor optimization.
                # For now, we assume bootstrap_nodes contains tuples that can be directly compared
                # or that exact string matches for hostnames are sufficient if they were used.
                is_bootstrap_node = False
                for bn_host, bn_port in self.bootstrap_nodes:
                    if bn_port == port:
                        # If ports match, we need to check hosts.
                        # This simple check won't resolve bn_host if it's a hostname.
                        # A proper implementation would resolve hostnames in bootstrap_nodes at init.
                        if bn_host == ip:
                            is_bootstrap_node = True
                            break

                if not is_bootstrap_node:
                    self.ping(addr=node_addr) # ping这些新发现的节点
                else:
                    logging.debug(f"Maga (log): Skipping ping for bootstrap node {node_addr} received in find_node response.")


    async def handle_query(self, msg, addr):
        # 处理DHT网络中的查询(query)消息。
        # query_type: 查询的类型 (e.g., "ping", "find_node", "get_peers", "announce_peer")
        args = msg.get(b"a", {}) # "a"字段包含查询参数
        node_id = args.get(b"id") # 请求节点的ID
        if not node_id: return # 无效请求

        query_type = msg.get(b"q") # "q"字段代表查询类型
        if not query_type: return # 无效请求

        # 根据查询类型进行不同处理
        if query_type == b"get_peers":
            # 当收到get_peers请求时，表示有节点在寻找特定infohash的peer。
            infohash = args.get(b"info_hash")
            if not infohash: return
            infohash_hex = proper_infohash(infohash) # 规范化infohash
            token = infohash[:2] # 生成token，用于后续announce_peer的校验
            self.send_message({
                b"t": msg.get(b"t"), # 使用请求中的交易ID
                b"y": b"r", # 响应类型
                b"r": { # 响应内容
                    b"id": self.fake_node_id(node_id), # 伪装的节点ID
                    b"nodes": b"", # 通常在此阶段不返回nodes，而是返回token
                    b"token": token # 返回token
                }
            }, addr=addr)
            # 调用handler处理发现的infohash和peer地址
            await self.handle_get_peers(infohash_hex, addr)
        elif query_type == b"announce_peer":
            # 当收到announce_peer请求时，表示有节点声明它是一个特定infohash的peer。
            infohash = args.get(b"info_hash")
            if not infohash: return

            # TODO: 在实际应用中，需要验证token (`args.get(b"token")`)
            # 以确保announce_peer的合法性，防止DHT污染。

            tid = msg.get(b"t") # 交易ID
            self.send_message({
                b"t": tid,
                b"y": b"r",
                b"r": {
                    b"id": self.fake_node_id(node_id)
                }
            }, addr=addr)

            # 获取peer的地址，优先使用announce_peer中声明的port
            peer_ip = addr[0]
            try:
                # "implied_port"参数指示是否使用来源端口
                if args.get(b"implied_port", 0) == 1:
                    peer_port = addr[1]
                else:
                    peer_port = args.get(b"port")
                    if not peer_port or not (0 < peer_port < 65536): # 验证端口有效性
                        return
            except KeyError:
                return # 如果没有port信息则忽略

            peer_actual_addr = (peer_ip, peer_port)
            # 调用handler处理发现的infohash和peer地址
            await self.handle_announce_peer(proper_infohash(infohash), addr, peer_actual_addr)
        elif query_type == b"find_node":
            # 处理find_node请求，返回一些已知的节点（此处简化，仅返回空节点列表）
            tid = msg.get(b"t")
            self.send_message({
                b"t": tid,
                b"y": b"r",
                b"r": {
                    b"id": self.fake_node_id(node_id),
                    b"nodes": b"" # 理论上应返回一些接近目标ID的节点
                }
            }, addr=addr)
        elif query_type == b"ping":
            # 处理ping请求，简单回复一个pong
            self.send_message({
                b"t": msg.get(b"t", b"pg"), # 使用请求的交易ID，或默认值
                b"y": b"r",
                b"r": {
                    b"id": self.fake_node_id(node_id)
                }
            }, addr=addr)
        # 对于所有有效的查询，都尝试将查询方节点加入到自己的路由表中
        self.find_node(addr=addr, node_id=node_id)

    def ping(self, addr, node_id=None):
        # 发送ping请求到指定地址。
        self.send_message({
            b"y": b"q",      # "q" 表示查询
            b"t": b"pg",     # "pg" 作为ping请求的自定义交易ID
            b"q": b"ping",   # "ping" 是查询类型
            b"a": {          # "a" 包含查询参数
                b"id": self.fake_node_id(node_id) # 发送伪装的节点ID
            }
        }, addr=addr)

    def connection_made(self, transport):
        # 当UDP端点创建成功时由asyncio调用。
        self.transport = transport

    def connection_lost(self, exc):
        # 当连接丢失或关闭时由asyncio调用。
        self.__running = False # 设置运行状态为False
        if self.transport:
            self.transport.close() # 关闭transport

    def send_message(self, data, addr):
        # 发送bencoded编码的消息到指定地址。
        # data: 要发送的字典数据
        # addr: 目标地址 (ip, port)
        data.setdefault(b"t", b"tt") # 如果没有提供交易ID "t"，则设置一个默认值
        if self.transport: # 确保transport可用
            self.transport.sendto(bencoder.bencode(data), addr)

    def fake_node_id(self, node_id=None):
        # 生成一个伪装的节点ID。
        # DHT协议要求节点ID与其IP地址相关联，此处通过替换真实ID的最后一部分来伪装。
        # 这有助于防止某些节点将我们的爬虫节点识别为固定节点。
        if node_id and len(node_id) == 20:
            return node_id[:-1]+self.node_id[-1:] # 用自己的node_id的最后一位替换对方node_id的最后一位
        return self.node_id # 如果没有提供node_id，则返回自己的node_id

    def find_node(self, addr, node_id=None, target=None):
        # 发送find_node请求到指定地址，以发现新的DHT节点。
        # target: 要查找的目标节点ID，如果未提供，则生成一个随机ID。
        if not target:
            target = random_node_id() # 生成随机目标ID
        self.send_message({
            b"t": b"fn",     # "fn" 作为find_node请求的自定义交易ID
            b"y": b"q",
            b"q": b"find_node",
            b"a": {
                b"id": self.fake_node_id(node_id), # 发送伪装的节点ID
                b"target": target # 要查找的目标节点ID
            }
        }, addr=addr)

    async def handle_get_peers(self, infohash, dht_node_addr):
        # 当从某个DHT节点(dht_node_addr)收到get_peers请求时，
        # dht_node_addr 本身就是潜在的peer。
        # 调用新的handler来尝试从这个peer获取元数据。
        # print(f"Get_peers: infohash {infohash} from DHT node {dht_node_addr}")
        await self.handler(infohash, dht_node_addr, dht_node_addr)

    async def handle_announce_peer(self, infohash, dht_node_addr, peer_actual_addr):
        # 当从某个DHT节点(dht_node_addr)收到announce_peer请求时，
        # peer_actual_addr 是实际声明拥有该infohash的peer的地址。
        # 调用新的handler来尝试从这个peer获取元数据。
        # print(f"Announce_peer: infohash {infohash} from DHT node {dht_node_addr} for peer {peer_actual_addr}")
        await self.handler(infohash, dht_node_addr, peer_actual_addr)

    async def handler(self, infohash: str, dht_node_addr: tuple, peer_addr: tuple):
        # 新的处理程序，负责调用get_metadata来获取元数据。
        # infohash: 目标种子的infohash (十六进制字符串)
        # dht_node_addr: 提供peer信息的DHT节点的地址 (ip, port)
        # peer_addr: 实际peer的地址 (ip, port)

        # Acquire the semaphore before attempting to get metadata
        async with self.metadata_semaphore:
            logging.info(f"Handler (log): Acquired semaphore. Attempting to get metadata for {infohash} from peer {peer_addr} (discovered via {dht_node_addr})")
            try:
                logging.getLogger().handlers[0].flush() # FLUSH LOGS
            except:
                pass # Ignore if no handlers or other issue
            # 将十六进制的infohash转回字节串给get_metadata
            try:
                infohash_bytes = binascii.unhexlify(infohash)
            except binascii.Error as e:
                logging.warning(f"Handler (log): Invalid infohash format for {infohash}: {e}")
                return

            try:
                # Pass our node_id as my_id to get_metadata
                metadata = await get_metadata(infohash_bytes, self.node_id, peer_addr[0], peer_addr[1], loop=self.loop)
                # 将获取到的元数据（或None）传递给子类实现的处理方法
                await self.process_metadata_result(metadata, infohash, peer_addr)
            except Exception as e:
                logging.error(f"Handler (log): Exception during get_metadata or process_metadata_result for {infohash} from {peer_addr}: {e}", exc_info=True)
            # Semaphore is released automatically by 'async with'
            logging.debug(f"Handler (log): Released semaphore for {infohash} from {peer_addr}")


    async def process_metadata_result(self, metadata: bytes | None, infohash: str, peer_addr: tuple):
        # 这是一个占位符方法 (或可视为抽象方法)，期望子类覆盖此方法来处理获取到的元数据。
        # metadata: 获取到的元数据内容 (bytes)，如果失败则为None。
        # infohash: 相关的infohash (十六进制字符串)。
        # peer_addr: 提供元数据的peer的地址 (ip, port)。
        # print(f"Process_metadata_result: infohash={infohash}, peer={peer_addr}, metadata_present={metadata is not None}")
        pass # 子类应该实现具体的处理逻辑，例如保存元数据、打印信息等。