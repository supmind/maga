# DHT 爬虫单一文件版本
# coding:utf-8

# 标准库导入
import asyncio
import binascii
import hashlib
import logging
import math
import os
import signal
import struct  # struct.unpack 和 struct.pack 都会用到
from socket import inet_ntoa

# 第三方库导入
import bencoder

# --- 全局日志记录器 ---
logger = logging.getLogger(__name__)

# --- 来自 maga.py 的常量 ---
__version__ = '3.0.0' # 版本号

BOOTSTRAP_NODES = ( # 引导节点
    # BitTorrent官方路由节点
    ("router.bittorrent.com", 6881),
    # Transmission客户端使用的DHT节点
    ("dht.transmissionbt.com", 6881),
    # uTorrent客户端使用的DHT节点
    ("router.utorrent.com", 6881)
)

# --- 来自 wire_protocol.py 的常量 ---
# BitTorrent协议标识符
BT_PROTOCOL = b"BitTorrent protocol"
# BitTorrent协议标识符的长度
BT_PROTOCOL_LEN = len(BT_PROTOCOL)
# 扩展协议ID，用于支持扩展消息
EXT_ID = 0
# 扩展握手消息ID
EXT_HANDSHAKE_ID = 0
# 扩展握手消息内容，通常包含客户端版本等信息
EXT_HANDSHAKE_MESSAGE = {"m": {"ut_metadata": 1}, "v": "python-bittorrent-v0.1"}
# 数据块常量，此处保留原文含义
BLOCK = 1
# 最大数据块大小，通常为16KB (2^14 bytes)
MAX_SIZE = 2 ** 14
# BitTorrent协议头部信息，用于构建握手消息
BT_HEADER = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x01"


# --- 工具函数 ---
def proper_infohash(infohash):
    """
    确保infohash是规范的大写十六进制字符串。
    如果输入是字节串，则将其转换为十六进制字符串。
    (来自maga.py)
    """
    if isinstance(infohash, bytes):
        infohash = binascii.hexlify(infohash).decode('utf-8')
    return infohash.upper()

def random_node_id(size=20): # DHT 节点ID
    """
    生成指定大小的随机节点ID。
    (来自maga.py)
    """
    return os.urandom(size)

def random_peer_id() -> bytes: # Peer ID (BEP 20)
    """
    生成一个随机的20字节ID，通常用作peer_id。
    (原名 random_id from wire_protocol.py)
    """
    return os.urandom(20)

def split_nodes(nodes: bytes):
    """
    分割紧凑编码的节点信息。
    节点信息是26字节的倍数，每个节点包含20字节的ID，4字节的IP和2字节的端口。
    (来自maga.py)
    """
    length = len(nodes)
    if (length % 26) != 0: # 长度必须是26的倍数
        return

    for i in range(0, length, 26):
        nid = nodes[i:i+20] # 节点ID
        ip = inet_ntoa(nodes[i+20:i+24]) # IP地址
        port = struct.unpack("!H", nodes[i+24:i+26])[0] # 端口号，网络字节序，无符号短整型
        yield nid, ip, port

def get_ut_metadata(data: dict) -> int:
    """
    从扩展握手消息中获取ut_metadata的值。
    (来自wire_protocol.py)
    """
    # 注意：bencoder解码后，字典的键是字节串
    return data.get(b"m", {}).get(b"ut_metadata", 0)

def get_metadata_size(data: dict) -> int:
    """
    从扩展握手消息中获取metadata_size的值。
    (来自wire_protocol.py)
    """
    # 注意：bencoder解码后，字典的键是字节串
    return data.get(b"metadata_size", 0)

# --- 来自 wire_protocol.py 的 MessageType 类 ---
class MessageType:
    HANDSHAKE = -2
    KEEP_ALIVE = -1
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8
    PORT = 9
    EXTENSION = 20

# --- WirePeerClient 类 (来自 wire_protocol.py) ---
class WirePeerClient:
    """
    WirePeerClient 类负责与单个peer进行网络通信，遵循BitTorrent协议。
    它处理消息的发送和接收，以及握手和数据交换逻辑，主要用于获取Torrent元数据。
    """
    def __init__(self, info_hash: bytes, my_id: bytes, host: str, port: int,
                 on_metadata_complete_cb, loop):
        self.info_hash = info_hash
        self.my_id = my_id
        self.host = host
        self.port = port
        self.on_metadata_complete_cb = on_metadata_complete_cb
        self.loop = loop
        self.queue = asyncio.Queue()
        self.remote_id = None
        self.handshake_send = False
        self.handshake_recv = False
        self.ext_handshake_send = False
        self.ext_handshake_recv = False
        self.interested_send = False
        self.unchoke_recv = False
        self.closing = False
        self.metadata_data_list = []
        self.metadata_size_val = 0
        self.metadata_total_pieces = 0
        self.metadata_downloaded_pieces = set()
        self.ut_metadata_id = 0
        self.reader = None
        self.writer = None

    async def read_messages(self) -> None:
        """持续从远端peer读取消息并放入队列。"""
        while not self.closing:
            try:
                data = await asyncio.wait_for(self.reader.readexactly(4), timeout=30)
                msg_len = struct.unpack(">I", data)[0]
                if msg_len == 0:
                    await self.queue.put((MessageType.KEEP_ALIVE, None))
                    continue
                data = await asyncio.wait_for(self.reader.readexactly(msg_len), timeout=30)
                msg_id = data[0]
                payload = data[1:]
                logger.debug(f"从 {self.host}:{self.port} 收到消息类型 {msg_id}，负载长度 {len(payload)}")
                await self.queue.put((msg_id, payload))
            except (ConnectionResetError, ConnectionRefusedError, asyncio.exceptions.IncompleteReadError, asyncio.TimeoutError, OSError) as e:
                logger.warning(f"从 {self.host}:{self.port} 读取消息时出错: {e}")
                await self.close()
                break
            except Exception as e:
                logger.error(f"从 {self.host}:{self.port} 读取消息时发生未知错误: {e}", exc_info=True)
                await self.close()
                break

    async def send_message(self, msg_id: int, data: bytes = None) -> None:
        """向远端peer发送消息。"""
        if self.closing or not self.writer or self.writer.is_closing(): return
        try:
            if msg_id == MessageType.HANDSHAKE:
                self.writer.write(data)
                await self.writer.drain()
                self.handshake_send = True
            elif msg_id == MessageType.KEEP_ALIVE:
                self.writer.write(struct.pack(">I", 0))
                await self.writer.drain()
            else:
                msg_len = 1 + (len(data) if data else 0)
                self.writer.write(struct.pack(">IB", msg_len, msg_id))
                if data: self.writer.write(data)
                await self.writer.drain()
        except (ConnectionResetError, ConnectionRefusedError, asyncio.TimeoutError, OSError, AttributeError) as e:
            logger.warning(f"向 {self.host}:{self.port} 发送消息时出错: {e}")
            await self.close()
        except Exception as e:
            logger.error(f"向 {self.host}:{self.port} 发送消息时发生未知错误: {e}", exc_info=True)
            await self.close()

    async def _connect(self) -> bool:
        """建立与远端peer的TCP连接。"""
        try:
            fut = asyncio.open_connection(self.host, self.port)
            self.reader, self.writer = await asyncio.wait_for(fut, timeout=5)
            logger.debug(f"成功连接到 {self.host}:{self.port}")
            asyncio.ensure_future(self.read_messages(), loop=self.loop)
            return True
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
            logger.warning(f"连接到 {self.host}:{self.port} 失败: {e}")
            return False
        except Exception as e:
            logger.error(f"连接到 {self.host}:{self.port} 失败: {e}", exc_info=True)
            return False

    async def do_handshake(self) -> bool:
        """执行与远端peer的握手过程。"""
        if not await self._connect(): return False
        handshake_msg = BT_HEADER + self.info_hash + self.my_id
        await self.send_message(MessageType.HANDSHAKE, handshake_msg)
        logger.debug(f"已向 {self.host}:{self.port} 发送握手消息")
        try:
            data = await asyncio.wait_for(self.reader.readexactly(BT_PROTOCOL_LEN + 1 + 8 + 20 + 20), timeout=10)
        except (asyncio.exceptions.IncompleteReadError, asyncio.TimeoutError, ConnectionResetError) as e:
            logger.warning(f"与 {self.host}:{self.port} 的握手在读取期间失败: {e}")
            await self.close(); return False
        except Exception as e:
            logger.error(f"与 {self.host}:{self.port} 的握手在读取期间发生未知错误: {e}", exc_info=True)
            await self.close(); return False
        protocol_len = data[0]
        if protocol_len != BT_PROTOCOL_LEN:
            logger.warning(f"与 {self.host}:{self.port} 的握手协议长度不匹配。期望 {BT_PROTOCOL_LEN}，得到 {protocol_len}")
            await self.close(); return False
        protocol_name = data[1:1 + protocol_len]
        if protocol_name != BT_PROTOCOL:
            logger.warning(f"与 {self.host}:{self.port} 的握手协议名称不匹配。期望 {BT_PROTOCOL}，得到 {protocol_name}")
            await self.close(); return False
        info_hash_recv = data[1 + protocol_len + 8:1 + protocol_len + 8 + 20]
        if info_hash_recv != self.info_hash:
            logger.warning(f"与 {self.host}:{self.port} 的握手info_hash不匹配")
            await self.close(); return False
        self.remote_id = data[1 + protocol_len + 8 + 20:1 + protocol_len + 8 + 20 + 20]
        self.handshake_recv = True
        logger.debug(f"与 {self.host}:{self.port} 的握手成功。远端ID: {binascii.hexlify(self.remote_id).decode()}")
        return True

    async def send_ext_handshake(self) -> None:
        """发送扩展握手消息。"""
        if not self.handshake_recv or self.ext_handshake_send: return
        payload = bencoder.encode(EXT_HANDSHAKE_MESSAGE)
        await self.send_message(MessageType.EXTENSION, bytes([EXT_HANDSHAKE_ID]) + payload)
        self.ext_handshake_send = True
        logger.debug(f"已向 {self.host}:{self.port} 发送扩展握手消息")

    async def send_interested(self) -> None:
        """发送Interested消息。"""
        if not self.ext_handshake_recv or self.interested_send or not self.ut_metadata_id: return
        await self.send_message(MessageType.INTERESTED)
        self.interested_send = True
        logger.debug(f"已向 {self.host}:{self.port} 发送Interested消息")

    async def request_metadata_piece(self, piece_index: int) -> None:
        """请求指定的元数据块。"""
        if not self.ut_metadata_id or not self.unchoke_recv: return
        payload = bencoder.encode({"msg_type": 0, "piece": piece_index})
        await self.send_message(MessageType.EXTENSION, bytes([self.ut_metadata_id]) + payload)
        logger.debug(f"已向 {self.host}:{self.port} 请求元数据块 {piece_index}")

    async def handle_message(self, msg_id: int, payload: bytes) -> None:
        """处理从远端peer接收到的消息。"""
        if msg_id == MessageType.CHOKE:
            logger.debug(f"从 {self.host}:{self.port} 收到 CHOKE")
            self.unchoke_recv = False
        elif msg_id == MessageType.UNCHOKE:
            logger.debug(f"从 {self.host}:{self.port} 收到 UNCHOKE")
            self.unchoke_recv = True
            if self.metadata_total_pieces > 0 and len(self.metadata_downloaded_pieces) < self.metadata_total_pieces:
                for i in range(self.metadata_total_pieces):
                    if i not in self.metadata_downloaded_pieces: await self.request_metadata_piece(i); break
        elif msg_id == MessageType.EXTENSION:
            ext_msg_id = payload[0]
            ext_payload_raw = payload[1:]
            if ext_msg_id == EXT_HANDSHAKE_ID:
                self.ext_handshake_recv = True
                try:
                    decoded_payload = bencoder.decode(ext_payload_raw)
                    self.ut_metadata_id = get_ut_metadata(decoded_payload)
                    self.metadata_size_val = get_metadata_size(decoded_payload)
                    if self.metadata_size_val > 0 and self.ut_metadata_id > 0:
                        self.metadata_total_pieces = math.ceil(self.metadata_size_val / MAX_SIZE)
                        self.metadata_data_list = [b""] * self.metadata_total_pieces
                        logger.debug(f"来自 {self.host}:{self.port} 的扩展握手。 ut_metadata_id: {self.ut_metadata_id}, metadata_size: {self.metadata_size_val}, 总块数: {self.metadata_total_pieces}")
                        await self.send_interested()
                    else:
                        logger.warning(f"Peer {self.host}:{self.port} 不支持元数据交换 (ut_metadata_id: {self.ut_metadata_id}) 或元数据大小无效 ({self.metadata_size_val})。正在关闭。")
                        await self.close()
                except Exception as e:
                    logger.error(f"从 {self.host}:{self.port} 解码扩展握手时出错: {e}", exc_info=True)
                    await self.close()
            elif ext_msg_id == self.ut_metadata_id and self.ut_metadata_id > 0:
                try:
                    metadata_piece_offset = ext_payload_raw.find(b"ee") + 2
                    if metadata_piece_offset == 1: # This means b"ee" was not found (find returned -1)
                        logger.warning(f"来自 {self.host}:{self.port} 的元数据块格式无效：未找到 'ee' 分隔符。正在关闭。")
                        await self.close()
                        return
                    # if metadata_piece_offset <= 1:
                    #      logger.warning(f"来自 {self.host}:{self.port} 的元数据块格式无效：未找到 'ee' 或其位置不正确。")
                    #      await self.close(); return
                    decoded_header = bencoder.decode(ext_payload_raw[:metadata_piece_offset])
                    piece_index = decoded_header.get(b"piece", -1)
                    actual_metadata_block = ext_payload_raw[metadata_piece_offset:]
                    if not (0 <= piece_index < self.metadata_total_pieces):
                        logger.warning(f"来自 {self.host}:{self.port} 的无效块索引 {piece_index}。总块数: {self.metadata_total_pieces}。正在关闭。")
                        await self.close(); return
                    expected_size = MAX_SIZE
                    if piece_index == self.metadata_total_pieces -1: expected_size = self.metadata_size_val - (MAX_SIZE * piece_index)
                    if len(actual_metadata_block) != expected_size:
                        logger.warning(f"元数据块 {piece_index} 大小与 {self.host}:{self.port} 不匹配。期望 {expected_size}，得到 {len(actual_metadata_block)}。正在关闭。")
                        await self.close(); return
                    if piece_index not in self.metadata_downloaded_pieces:
                        self.metadata_data_list[piece_index] = actual_metadata_block
                        self.metadata_downloaded_pieces.add(piece_index)
                        logger.debug(f"从 {self.host}:{self.port} 收到元数据块 {piece_index}。已下载 {len(self.metadata_downloaded_pieces)}/{self.metadata_total_pieces}")
                        if len(self.metadata_downloaded_pieces) == self.metadata_total_pieces:
                            full_metadata = b"".join(self.metadata_data_list)
                            if hashlib.sha1(full_metadata).digest() == self.info_hash:
                                logger.info(f"{binascii.hexlify(self.info_hash).decode()} 的元数据已成功从 {self.host}:{self.port} 下载")
                                if self.on_metadata_complete_cb: self.on_metadata_complete_cb(full_metadata)
                                await self.close()
                            else:
                                logger.warning(f"{binascii.hexlify(self.info_hash).decode()} 的元数据哈希与 {self.host}:{self.port} 不匹配。数据损坏。正在关闭。")
                                self.metadata_downloaded_pieces.clear(); self.metadata_data_list = [b""] * self.metadata_total_pieces
                                await self.close()
                        elif self.unchoke_recv:
                            for i in range(self.metadata_total_pieces):
                                if i not in self.metadata_downloaded_pieces: await self.request_metadata_piece(i); break
                    else: logger.debug(f"已拥有来自 {self.host}:{self.port} 的块 {piece_index}，忽略。")
                except Exception as e:
                    logger.error(f"从 {self.host}:{self.port} 处理元数据块时出错: {e}", exc_info=True)
                    await self.close()

    async def close(self) -> None:
        """关闭与远端peer的连接。"""
        if self.closing: return
        self.closing = True
        logger.debug(f"正在关闭与 {self.host}:{self.port} 的连接 (关闭标志: {self.closing})")
        if self.writer:
            self.writer.close()
            try: await self.writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError, OSError, AttributeError) as e: logger.debug(f"{self.host}:{self.port} 的 writer.wait_closed() 期间出错: {e}")
            except Exception as e: logger.warning(f"{self.host}:{self.port} 的 writer.wait_closed() 期间发生意外错误: {e}", exc_info=True)
        self.writer = None; self.reader = None
        logger.debug(f"与 {self.host}:{self.port} 的连接流已关闭。")
        while True:
            try: self.queue.get_nowait(); self.queue.task_done()
            except asyncio.QueueEmpty: break
            except Exception as e: logger.warning(f"为 {self.host}:{self.port} 清理队列时发生意外错误: {e}", exc_info=True); break
        logger.debug(f"为 {self.host}:{self.port} 清理消息队列完成。")

    async def start(self) -> None:
        """启动与peer的通信。"""
        logger.debug(f"正在为 {self.host}:{self.port} 启动客户端，info_hash {binascii.hexlify(self.info_hash).decode()}")
        if not await self.do_handshake():
            logger.info(f"与 {self.host}:{self.port} 握手失败，正在关闭连接。")
            return
        await self.send_ext_handshake()
        try:
            await asyncio.wait_for(self._message_loop(), timeout=60)
        except asyncio.TimeoutError: logger.info(f"与 {self.host}:{self.port} 的元数据交换总体超时。")
        except Exception as e: logger.error(f"{self.host}:{self.port} 的消息循环中发生意外错误: {e}", exc_info=True)
        finally:
            logger.debug(f"{self.host}:{self.port} 的客户端 start() 方法结束，确保关闭。")
            await self.close()

    async def _message_loop(self):
        """内部消息处理循环。"""
        while not self.closing:
            if self.metadata_total_pieces > 0 and len(self.metadata_downloaded_pieces) == self.metadata_total_pieces:
                logger.debug(f"{self.host}:{self.port} 元数据下载完成，退出消息循环。"); break
            try:
                msg_id, payload = await asyncio.wait_for(self.queue.get(), timeout=30)
                await self.handle_message(msg_id, payload)
                self.queue.task_done()
            except asyncio.TimeoutError:
                if self.closing: logger.debug(f"{self.host}:{self.port} 的消息循环超时，但已在关闭。"); break
                logger.debug(f"等待来自 {self.host}:{self.port} 队列的消息超时。")
                if self.handshake_recv:
                    if self.ext_handshake_send and not self.ext_handshake_recv and self.metadata_total_pieces == 0:
                        logger.warning(f"等待来自 {self.host}:{self.port} 的扩展握手响应超时，正在关闭。"); await self.close()
                    elif self.interested_send and not self.unchoke_recv and self.metadata_total_pieces > 0:
                        logger.warning(f"发送INTERESTED后等待来自 {self.host}:{self.port} 的UNCHOKE超时，正在关闭。"); await self.close()
                    elif self.metadata_total_pieces > 0 and len(self.metadata_downloaded_pieces) < self.metadata_total_pieces:
                        logger.warning(f"等待来自 {self.host}:{self.port} 的元数据块超时，正在关闭。"); await self.close()
                    else: logger.debug(f"向 {self.host}:{self.port} 发送keep-alive。"); await self.send_message(MessageType.KEEP_ALIVE)
                elif not self.handshake_recv: logger.warning(f"等待来自 {self.host}:{self.port} 的初始消息（握手前）超时，正在关闭。"); await self.close()
            except Exception as e: logger.error(f"{self.host}:{self.port} 的消息循环中出错: {e}", exc_info=True); await self.close(); break

# --- get_metadata 函数 (来自 wire_protocol.py) ---
async def get_metadata(info_hash: bytes, my_id: bytes, host: str, port: int, loop) -> bytes | None:
    """
    尝试从单个peer处获取Torrent的元数据。
    它会创建一个WirePeerClient实例，执行握手、扩展握手，并请求元数据块。
    如果成功获取，返回元数据内容(bytes)；否则返回None。
    """
    metadata_container = [None]
    def on_metadata_complete(metadata_content: bytes):
        metadata_container[0] = metadata_content
    client = WirePeerClient(info_hash, my_id, host, port, on_metadata_complete, loop)
    try:
        await client.start()
    except Exception as e:
        logger.error(f"调用 get_metadata 获取 {host}:{port} (info_hash: {binascii.hexlify(info_hash).decode()}) 时发生异常: {e}", exc_info=True)
        if not client.closing: await client.close()
    return metadata_container[0]

# --- Maga 类 (来自 maga.py) ---
# Maga类是实现DHT协议的爬虫核心。
# 它通过加入DHT网络，监听其他节点发送的get_peers和announce_peer请求，
# 从而发现infohash和对应的peer地址。
# 新版本集成了wire_protocol.py中的功能，使其能够主动连接peer并尝试下载元数据。
class Maga(asyncio.DgramProtocol):
    def __init__(self, loop=None, bootstrap_nodes=BOOTSTRAP_NODES, interval=1, max_concurrent_metadata_fetches=20): # 添加了 max_concurrent_metadata_fetches
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
        # 初始化信号量
        self.metadata_semaphore = asyncio.Semaphore(max_concurrent_metadata_fetches)
        self.secret_salt = os.urandom(16) # Add this line in __init__
        logger.info(f"Maga (日志): 初始化完成，max_concurrent_metadata_fetches={max_concurrent_metadata_fetches}")

    def _generate_token(self, infohash_bytes: bytes, ip_address_str: str) -> bytes:
        # Helper method to generate a consistent token for get_peers/announce_peer
        h = hashlib.sha1()
        h.update(infohash_bytes)
        h.update(ip_address_str.encode('utf-8')) # Encode IP to bytes
        h.update(self.secret_salt)
        return h.digest()[:4] # Return the first 4 bytes of the hash as token

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
        logger.info(f"Maga (日志): 在端口 {port} 上启动DHT节点... (原标准输出)")
        try:
            # 确保日志在启动时被刷新（如果处理程序存在）
            if logger.handlers: # 检查是否有处理器
                 logger.handlers[0].flush() # 刷新日志
        except Exception: # 忽略处理器不存在或其他问题
            pass
        coro = self.loop.create_datagram_endpoint(
                lambda: self, local_addr=('0.0.0.0', port) # 创建UDP端点
        )
        asyncio.set_event_loop(self.loop) # 设置当前事件循环
        self.transport, _ = self.loop.run_until_complete(coro) # 运行直到端点创建完成

        # 注册信号处理器，用于优雅地停止程序
        for signame in ('SIGINT', 'SIGTERM'):
            try:
                self.loop.add_signal_handler(getattr(signal, signame), self.stop)
            except NotImplementedError:
                # Windows上可能不支持某些信号
                pass

        # 向引导节点发送find_node请求以加入DHT网络
        for node in self.bootstrap_nodes:
            # 引导过程
            self.find_node(addr=node, node_id=self.node_id)

        self.loop.create_task(self.auto_find_nodes()) # 启动自动查找节点的任务
        self.loop.run_forever() # 持续运行事件循环
        self.loop.close() # 关闭事件循环

    def datagram_received(self, data, addr):
        # 当接收到UDP数据报时由asyncio调用。
        # data: 收到的字节数据
        # addr: 发送方的地址 (ip, port)
        logger.debug(f"Maga (日志): 从 {addr} 收到 {len(data)} 字节数据")
        try:
            # 确保日志被刷新（如果处理程序存在）
            if logger.handlers: # 检查是否有处理器
                logger.handlers[0].flush() # 刷新日志
        except Exception: # 忽略处理器不存在或其他问题
            pass
        try:
            msg = bencoder.bdecode(data) # bdecode解码收到的数据
        except Exception as e:
            logger.warning(f"Maga (日志): 从 {addr} 解码bencode数据失败: {e}")
            return
        try:
            self.handle_message(msg, addr) # 处理解码后的消息
        except Exception as e:
            logger.error(f"Maga (日志): 处理来自 {addr} 的消息时出错: {e}", exc_info=True)
            # 发生错误，向对方发送错误信息
            # msg["t"] 是交易ID，需要包含在响应中
            if msg and b"t" in msg: # 确保 msg 不为 None 且 't' 存在
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
                node_addr = (ip, port)
                is_bootstrap_node = False
                for bn_host, bn_port in self.bootstrap_nodes:
                    if bn_port == port and bn_host == ip: # 简单比较IP和端口
                        is_bootstrap_node = True
                        break
                if not is_bootstrap_node:
                    self.ping(addr=node_addr) # ping这些新发现的节点
                else:
                    logger.debug(f"Maga (日志): 跳过对在find_node响应中收到的引导节点 {node_addr} 的ping操作。")


    async def handle_query(self, msg, addr):
        # 处理DHT网络中的查询(query)消息。
        args = msg.get(b"a", {}) # "a"字段包含查询参数
        node_id = args.get(b"id") # 请求节点的ID
        if not node_id: return

        query_type = msg.get(b"q") # "q"字段代表查询类型
        if not query_type: return

        if query_type == b"get_peers":
            infohash = args.get(b"info_hash")
            if not infohash: return
            token = self._generate_token(infohash, addr[0]) # addr[0] is the IP string
            infohash_hex = proper_infohash(infohash) # This line is still needed for handle_get_peers
            self.send_message({
                b"t": msg.get(b"t"),
                b"y": b"r",
                b"r": {
                    b"id": self.fake_node_id(node_id),
                    b"nodes": b"",
                    b"token": token
                }
            }, addr=addr)
            await self.handle_get_peers(infohash_hex, addr)
        elif query_type == b"announce_peer":
            infohash = args.get(b"info_hash")
            if not infohash: return

            received_token = args.get(b"token")
            if not received_token:
                logger.debug(f"Maga (日志): 从 {addr} 收到的 announce_peer 请求缺少token，已忽略。Infohash: {proper_infohash(infohash)}")
                return

            expected_token = self._generate_token(infohash, addr[0])

            if received_token != expected_token:
                logger.warning(f"Maga (日志): 从 {addr} 收到的 announce_peer 请求token无效。收到: {binascii.hexlify(received_token).decode()}, 期望: {binascii.hexlify(expected_token).decode()}。Infohash: {proper_infohash(infohash)}")
                # Optionally, send an error response
                # self.send_message({
                #     b"t": msg.get(b"t"),
                #     b"y": b"e",
                #     b"e": [203, b"Protocol Error, bad token"]
                # }, addr=addr)
                return

            # logger.debug(f"Maga (日志): 从 {addr} 收到的 announce_peer 请求token有效。Infohash: {proper_infohash(infohash)}") # Optional: log successful token validation
            # Token验证通过，继续处理announce_peer
            tid = msg.get(b"t")
            self.send_message({
                b"t": tid,
                b"y": b"r",
                b"r": {b"id": self.fake_node_id(node_id)}
            }, addr=addr)
            peer_ip = addr[0]
            try:
                if args.get(b"implied_port", 0) == 1:
                    peer_port = addr[1]
                else:
                    peer_port = args.get(b"port")
                    if not peer_port or not (0 < peer_port < 65536): return
            except KeyError: return
            peer_actual_addr = (peer_ip, peer_port)
            await self.handle_announce_peer(proper_infohash(infohash), addr, peer_actual_addr)
        elif query_type == b"find_node":
            tid = msg.get(b"t")
            self.send_message({
                b"t": tid,
                b"y": b"r",
                b"r": {
                    b"id": self.fake_node_id(node_id),
                    b"nodes": b""
                }
            }, addr=addr)
        elif query_type == b"ping":
            self.send_message({
                b"t": msg.get(b"t", b"pg"),
                b"y": b"r",
                b"r": {b"id": self.fake_node_id(node_id)}
            }, addr=addr)
        self.find_node(addr=addr, node_id=node_id)

    def ping(self, addr, node_id=None):
        # 发送ping请求到指定地址。
        self.send_message({
            b"y": b"q",
            b"t": b"pg",
            b"q": b"ping",
            b"a": {b"id": self.fake_node_id(node_id)}
        }, addr=addr)

    def connection_made(self, transport):
        # 当UDP端点创建成功时由asyncio调用。
        self.transport = transport

    def connection_lost(self, exc):
        # 当连接丢失或关闭时由asyncio调用。
        self.__running = False
        if self.transport:
            self.transport.close()

    def send_message(self, data, addr):
        # 发送bencoded编码的消息到指定地址。
        data.setdefault(b"t", b"tt")
        if self.transport:
            self.transport.sendto(bencoder.bencode(data), addr)

    def fake_node_id(self, node_id=None):
        # 生成一个伪装的节点ID。
        if node_id and len(node_id) == 20:
            return node_id[:-1]+self.node_id[-1:]
        return self.node_id

    def find_node(self, addr, node_id=None, target=None):
        # 发送find_node请求到指定地址。
        if not target:
            target = random_node_id()
        self.send_message({
            b"t": b"fn",
            b"y": b"q",
            b"q": b"find_node",
            b"a": {
                b"id": self.fake_node_id(node_id),
                b"target": target
            }
        }, addr=addr)

    async def handle_get_peers(self, infohash, dht_node_addr):
        # 当从某个DHT节点收到get_peers请求时，dht_node_addr本身就是潜在的peer。
        # print(f"收到Get_peers: infohash {infohash} 来自DHT节点 {dht_node_addr}")
        await self.handler(infohash, dht_node_addr, dht_node_addr)

    async def handle_announce_peer(self, infohash, dht_node_addr, peer_actual_addr):
        # 当从某个DHT节点收到announce_peer请求时，peer_actual_addr是实际声明拥有该infohash的peer的地址。
        # print(f"收到Announce_peer: infohash {infohash} 来自DHT节点 {dht_node_addr}，实际peer地址 {peer_actual_addr}")
        await self.handler(infohash, dht_node_addr, peer_actual_addr)

    async def handler(self, infohash: str, dht_node_addr: tuple, peer_addr: tuple):
        # 新的处理程序，负责调用get_metadata来获取元数据。
        async with self.metadata_semaphore: # 在尝试获取元数据前获取信号量
            logger.info(f"Handler (日志): 已获取信号量。尝试从peer {peer_addr} (经由 {dht_node_addr} 发现) 获取 {infohash} 的元数据")
            try:
                # 确保日志被刷新
                if logger.handlers: logger.handlers[0].flush()
            except Exception: pass
            try:
                infohash_bytes = binascii.unhexlify(infohash) # 十六进制infohash转字节串
            except binascii.Error as e:
                logger.warning(f"Handler (日志): {infohash} 的infohash格式无效: {e}")
                return

            try:
                # 将 self.node_id 作为 my_id 传递给 get_metadata
                metadata = await get_metadata(infohash_bytes, self.node_id, peer_addr[0], peer_addr[1], loop=self.loop)
                await self.process_metadata_result(metadata, infohash, peer_addr) # 处理获取到的元数据
            except Exception as e:
                logger.error(f"Handler (日志): 从 {peer_addr} 获取 {infohash} 的元数据或处理结果时发生异常: {e}", exc_info=True)
            # 'async with' 会自动释放信号量
            logger.debug(f"Handler (日志): 已释放关于 {infohash} (来自 {peer_addr}) 的信号量")

    async def process_metadata_result(self, metadata: bytes | None, infohash: str, peer_addr: tuple):
        # 这是一个占位符方法，期望子类覆盖此方法来处理获取到的元数据。
        # metadata: 获取到的元数据内容 (bytes)，如果失败则为None。
        # infohash: 相关的infohash (十六进制字符串)。
        # peer_addr: 提供元数据的peer的地址 (ip, port)。
        # print(f"处理元数据结果: infohash={infohash}, peer={peer_addr}, 是否有元数据={metadata is not None}")
        pass # 子类应实现具体的处理逻辑

# --- Crawler 类 (来自 example.py, 继承自Maga) ---
# Crawler类继承自Maga类。
# Maga类处理了DHT协议的复杂性，如节点发现、请求路由等。
# Crawler类的主要目的是通过覆盖Maga类中的特定方法（现在是process_metadata_result）
# 来定义当通过DHT网络发现infohash和潜在的peer地址，并且成功（或失败）获取元数据后，
# 应该执行的具体操作。
class Crawler(Maga):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 此处的logger将使用全局logger的子logger，名称类似于 dht_crawler.Crawler (如果模块名为dht_crawler)
        # 或者直接使用全局logger: self.logger = logger (如果希望所有日志来自同一源)
        # 为保持与example.py一致，这里创建一个新的logger实例，它会继承父logger的配置。
        self.crawler_logger = logging.getLogger(__name__ + ".Crawler") # 创建一个特定的子logger
        self.successful_fetches = 0
        self.failed_fetches = 0
        self.last_successful_fetches = 0
        self.last_failed_fetches = 0

    # process_metadata_result 是一个回调方法，当Maga基类通过get_metadata获取到
    # 一个infohash对应的元数据（或获取失败）时，此方法会被调用。
    # metadata: 获取到的元数据内容 (bytes类型，通常是bencoded字典)，如果获取失败则为None。
    # infohash: 相关的infohash (十六进制字符串格式)。
    # peer_addr: 提供元数据的peer的地址，格式为 (ip, port) 元组。
    async def process_metadata_result(self, metadata: bytes | None, infohash: str, peer_addr: tuple):
        # 首先记录接收到的infohash和peer地址，无论元数据获取是否成功。
        # 注意: Maga基类如果被覆盖了process_metadata_result，它本身可能已经记录了尝试和成功/失败。
        # 我们可以保留这些日志，或依赖基类日志 + 统计数据。目前保留它们。
        self.crawler_logger.info(f"收到infohash结果: {infohash} 来自peer: {peer_addr}")

        if metadata:
            self.successful_fetches += 1
            # 如果metadata存在 (不是None)，说明元数据获取成功。
            self.crawler_logger.info(f"成功从 {peer_addr} 获取 {infohash} 的元数据。元数据大小: {len(metadata)} 字节。")
            # 例如，尝试解码并打印文件名（如果存在）
            try:
                # bencoder 已经在模块顶部导入
                decoded_metadata = bencoder.bdecode(metadata)
                if isinstance(decoded_metadata, dict):
                    name = decoded_metadata.get(b'name')
                    if name:
                        try:
                            self.crawler_logger.info(f"  名称: {name.decode('utf-8', errors='ignore')}")
                        except UnicodeDecodeError:
                            self.crawler_logger.info(f"  名称 (原始字节): {name}")
                    else:
                        self.crawler_logger.info(f"  元数据不包含 'name' 字段。")
                else:
                    self.crawler_logger.info(f"  元数据不是一个字典。")
            except Exception as e:
                self.crawler_logger.warning(f"  无法为 {infohash} 解码或处理元数据: {e}")
        else:
            self.failed_fetches += 1
            # 如果metadata为None，说明元数据获取失败。
            self.crawler_logger.warning(f"从 {peer_addr} 获取 {infohash} 的元数据失败。")

    async def print_stats(self, interval=60):
        """定期打印统计信息。"""
        self.crawler_logger.info(f"性能统计打印已启动。报告间隔: {interval}秒")
        while True:
            await asyncio.sleep(interval)
            current_success = self.successful_fetches
            current_failed = self.failed_fetches

            delta_success = current_success - self.last_successful_fetches
            delta_failed = current_failed - self.last_failed_fetches

            self.crawler_logger.info(
                f"统计: 成功 (最近{interval}秒): {delta_success}, "
                f"失败 (最近{interval}秒): {delta_failed}。 "
                f"总成功: {current_success}, 总失败: {current_failed}"
            )

            self.last_successful_fetches = current_success
            self.last_failed_fetches = current_failed

# --- Main Execution Block (来自 example.py) ---
if __name__ == '__main__':
    # 配置日志记录，设置级别为DEBUG，以捕获更详细的输出，包括Maga基类中的debug日志。
    # 注意：全局的 logger = logging.getLogger(__name__) 和 Crawler中的 self.crawler_logger 将继承此配置。
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 创建Crawler实例
    # 通过设置 max_concurrent_metadata_fetches 参数来演示，例如设置为5
    # 这个参数现在由Maga类的__init__处理
    crawler = Crawler(max_concurrent_metadata_fetches=5)

    # 调度 print_stats 任务
    # crawler.loop 在 Maga 的 __init__ 中被初始化
    # Maga类中的loop是在__init__中创建的，如果外部没有提供的话。
    # Crawler继承Maga，所以它也有self.loop
    if crawler.loop: # 确保loop存在
        crawler.loop.create_task(crawler.print_stats(interval=60))
    else:
        # 根据Maga的__init__逻辑，loop应该总是被创建的
        logging.error("事件循环 (loop) 未在Crawler中初始化，无法调度 print_stats。")


    # 运行爬虫，监听DHT网络的默认端口6881。
    # Maga基类的run方法会启动网络监听和DHT节点维护。
    # 当通过get_peers或announce_peer发现infohash，并尝试从peer获取元数据后，
    # Crawler中定义的process_metadata_result方法将被回调。
    logger.info("dht_crawler (日志): 初始化Crawler并开始运行... (原标准输出)") # 为控制台反馈保留此print语句
    try:
        # 尝试刷新日志处理器（如果存在）
        # 这在某些情况下有助于确保所有启动日志都已写出
        if logging.getLogger().handlers: # 检查root logger是否有处理器
            # 通常basicConfig会添加一个处理器到root logger
            for handler in logging.getLogger().handlers: # 遍历所有处理器
                handler.flush() # 刷新日志
        else:
            # 如果basicConfig没有添加处理器（不太可能，但为了稳健）
            # 或者我们想确保特定子logger的处理器被刷新
            if logger.handlers: # 检查我们模块级别的logger
                 for handler in logger.handlers: handler.flush()
            if crawler.crawler_logger.handlers: # 检查Crawler实例的logger
                 for handler in crawler.crawler_logger.handlers: handler.flush()

    except Exception as e:
        # 忽略：例如，如果没有配置处理器，或者发生其他与日志刷新相关的小问题
        logging.debug(f"刷新日志时的小问题（可忽略）: {e}")
        pass

    crawler.run(port=6881)
