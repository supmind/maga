# coding:utf-8
import logging
import os
import hashlib
import asyncio
import binascii
import struct
import bencoder
import math

class MessageType:
    # 握手消息的ID
    HANDSHAKE = -2
    # Keep-alive消息的ID
    KEEP_ALIVE = -1
    # Choke消息的ID，表示阻塞远端peer的请求
    CHOKE = 0
    # Unchoke消息的ID，表示解除对远端peer的阻塞
    UNCHOKE = 1
    # Interested消息的ID，表示对远端peer拥有数据块感兴趣
    INTERESTED = 2
    # Not Interested消息的ID，表示对远端peer拥有数据块不感兴趣
    NOT_INTERESTED = 3
    # Have消息的ID，通知远端peer本地已下载某数据块
    HAVE = 4
    # Bitfield消息的ID，用于交换各自拥有的数据块信息
    BITFIELD = 5
    # Request消息的ID，向远端peer请求数据块
    REQUEST = 6
    # Piece消息的ID，远端peer发送过来的数据块
    PIECE = 7
    # Cancel消息的ID，取消之前对数据块的请求
    CANCEL = 8
    # Port消息的ID，用于DHT网络，通知远端peer本地监听的端口号
    PORT = 9
    # 扩展消息的ID
    EXTENSION = 20

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
# 数据块常量，具体含义需结合上下文，可能表示请求或拥有数据块
BLOCK = 1
# 最大数据块大小，通常为16KB (2^14 bytes)
MAX_SIZE = 2 ** 14
# BitTorrent协议头部信息，用于构建握手消息
BT_HEADER = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x01"

logger = logging.getLogger(__name__)

def random_id() -> bytes:
    """
    生成一个随机的20字节ID，通常用作peer_id。
    Generates a random 20-byte ID, typically used as a peer_id.
    """
    return os.urandom(20)

def get_ut_metadata(data: dict) -> int:
    """
    从扩展握手消息中获取ut_metadata的值。
    Retrieves the ut_metadata value from the extended handshake message.
    """
    # 注意：bencoder解码后，字典的键是字节串
    return data.get(b"m", {}).get(b"ut_metadata", 0)

def get_metadata_size(data: dict) -> int:
    """
    从扩展握手消息中获取metadata_size的值。
    Retrieves the metadata_size value from the extended handshake message.
    """
    # 注意：bencoder解码后，字典的键是字节串
    return data.get(b"metadata_size", 0)

class WirePeerClient:
    """
    WirePeerClient 类负责与单个peer进行网络通信，遵循BitTorrent协议。
    它处理消息的发送和接收，以及握手和数据交换逻辑，主要用于获取Torrent元数据。
    The WirePeerClient class is responsible for network communication with a single peer,
    following the BitTorrent protocol. It handles sending and receiving messages,
    as well as handshake and data exchange logic, primarily for fetching Torrent metadata.
    """
    def __init__(self, info_hash: bytes, my_id: bytes, host: str, port: int,
                 on_metadata_complete_cb, loop):
        # Torrent的info_hash
        self.info_hash = info_hash
        # 本地客户端的peer_id
        self.my_id = my_id
        # 远端peer的IP地址
        self.host = host
        # 远端peer的端口号
        self.port = port
        # 元数据下载完成后的回调函数
        self.on_metadata_complete_cb = on_metadata_complete_cb
        # 事件循环
        self.loop = loop
        # 异步队列，用于存放从peer接收到的消息
        self.queue = asyncio.Queue()
        # 远端peer的ID
        self.remote_id = None
        # 标记是否已发送握手消息
        self.handshake_send = False
        # 标记是否已接收到握手消息
        self.handshake_recv = False
        # 标记是否已发送扩展握手消息
        self.ext_handshake_send = False
        # 标记是否已接收到扩展握手消息
        self.ext_handshake_recv = False
        # 标记是否已发送interested消息
        self.interested_send = False
        # 标记是否已接收到unchoke消息
        self.unchoke_recv = False
        # 标记是否正在关闭连接
        self.closing = False
        # 元数据字节数组列表，用于存储下载的元数据块
        self.metadata_data_list = []
        # 元数据总大小
        self.metadata_size_val = 0
        # 元数据块总数
        self.metadata_total_pieces = 0
        # 已下载的元数据块索引集合
        self.metadata_downloaded_pieces = set()
        # 远端peer支持的ut_metadata ID
        self.ut_metadata_id = 0
        # StreamReader对象，用于从socket读取数据
        self.reader = None
        # StreamWriter对象，用于向socket写入数据
        self.writer = None

    async def read_messages(self) -> None:
        """
        持续从远端peer读取消息并放入队列。
        Continuously reads messages from the remote peer and puts them into the queue.
        """
        while not self.closing:
            try:
                # 从socket读取4字节的消息长度前缀
                data = await asyncio.wait_for(self.reader.readexactly(4), timeout=30) # Timeout reduced to 30s
                msg_len = struct.unpack(">I", data)[0]

                if msg_len == 0:
                    # Keep-alive消息
                    await self.queue.put((MessageType.KEEP_ALIVE, None))
                    continue
                # 读取消息体
                data = await asyncio.wait_for(self.reader.readexactly(msg_len), timeout=30) # Timeout reduced to 30s
                msg_id = data[0]
                payload = data[1:]
                logger.debug(f"Received message type {msg_id} from {self.host}:{self.port} with payload length {len(payload)}")
                # 将消息放入队列
                await self.queue.put((msg_id, payload))
            except (ConnectionResetError, ConnectionRefusedError,
                    asyncio.exceptions.IncompleteReadError,
                    asyncio.TimeoutError, OSError) as e:
                logger.warning(f"Error reading messages from {self.host}:{self.port}: {e}")
                await self.close()
                break
            except Exception as e:
                logger.error(f"Unknown error reading messages from {self.host}:{self.port}: {e}", exc_info=True)
                await self.close()
                break

    async def send_message(self, msg_id: int, data: bytes = None) -> None:
        """
        向远端peer发送消息。
        Sends a message to the remote peer.
        """
        if self.closing or not self.writer or self.writer.is_closing():
            return

        try:
            if msg_id == MessageType.HANDSHAKE:
                # 发送握手消息
                self.writer.write(data)
                await self.writer.drain()
                self.handshake_send = True
            elif msg_id == MessageType.KEEP_ALIVE:
                # 发送Keep-alive消息
                self.writer.write(struct.pack(">I", 0))
                await self.writer.drain()
            else:
                # 发送其他类型的消息
                msg_len = 1 + (len(data) if data else 0)
                self.writer.write(struct.pack(">IB", msg_len, msg_id))
                if data:
                    self.writer.write(data)
                await self.writer.drain()
        except (ConnectionResetError, ConnectionRefusedError,
                asyncio.TimeoutError, OSError, AttributeError) as e:
            logger.warning(f"Error sending message to {self.host}:{self.port}: {e}")
            await self.close()
        except Exception as e:
            logger.error(f"Unknown error sending message to {self.host}:{self.port}: {e}", exc_info=True)
            await self.close()

    async def _connect(self) -> bool:
        """
        建立与远端peer的TCP连接。
        Establishes a TCP connection with the remote peer.
        """
        try:
            # 连接到远端peer
            fut = asyncio.open_connection(self.host, self.port)
            self.reader, self.writer = await asyncio.wait_for(fut, timeout=5) # 设置连接超时
            logger.debug(f"Successfully connected to {self.host}:{self.port}")
            # 启动读取消息的协程
            asyncio.ensure_future(self.read_messages(), loop=self.loop)
            return True
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
            logger.warning(f"Connection to {self.host}:{self.port} failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to {self.host}:{self.port}: {e}", exc_info=True)
            return False

    async def do_handshake(self) -> bool:
        """
        执行与远端peer的握手过程。
        Performs the handshake process with the remote peer.
        """
        if not await self._connect():
            return False

        # 构建并发送握手消息
        handshake_msg = BT_HEADER + self.info_hash + self.my_id
        await self.send_message(MessageType.HANDSHAKE, handshake_msg)
        logger.debug(f"Sent handshake to {self.host}:{self.port}")

        try:
            # 等待接收远端peer的握手响应
            data = await asyncio.wait_for(self.reader.readexactly(BT_PROTOCOL_LEN + 1 + 8 + 20 + 20), timeout=10)
        except (asyncio.exceptions.IncompleteReadError, asyncio.TimeoutError, ConnectionResetError) as e:
            logger.warning(f"Handshake with {self.host}:{self.port} failed during read: {e}")
            await self.close()
            return False
        except Exception as e:
            logger.error(f"Handshake with {self.host}:{self.port} unknown error during read: {e}", exc_info=True)
            await self.close()
            return False

        # 解析握手响应
        protocol_len = data[0]
        if protocol_len != BT_PROTOCOL_LEN:
            logger.warning(f"Handshake protocol length mismatch with {self.host}:{self.port}. Expected {BT_PROTOCOL_LEN}, got {protocol_len}")
            await self.close()
            return False

        protocol_name = data[1:1 + protocol_len]
        if protocol_name != BT_PROTOCOL:
            logger.warning(f"Handshake protocol name mismatch with {self.host}:{self.port}. Expected {BT_PROTOCOL}, got {protocol_name}")
            await self.close()
            return False

        # reserved_bytes = data[1 + protocol_len:1 + protocol_len + 8] # 保留字节
        info_hash_recv = data[1 + protocol_len + 8:1 + protocol_len + 8 + 20]
        if info_hash_recv != self.info_hash:
            logger.warning(f"Handshake info_hash mismatch with {self.host}:{self.port}")
            await self.close()
            return False

        # 获取远端peer的ID
        self.remote_id = data[1 + protocol_len + 8 + 20:1 + protocol_len + 8 + 20 + 20]
        self.handshake_recv = True
        logger.debug(f"Handshake with {self.host}:{self.port} successful. Remote ID: {binascii.hexlify(self.remote_id).decode()}")
        return True

    async def send_ext_handshake(self) -> None:
        """
        发送扩展握手消息。
        Sends an extended handshake message.
        """
        if not self.handshake_recv or self.ext_handshake_send:
            return
        # 构建扩展握手消息体
        payload = bencoder.encode(EXT_HANDSHAKE_MESSAGE)
        # 发送扩展消息，其中EXT_ID是扩展协议ID，EXT_HANDSHAKE_ID是扩展握手消息ID
        await self.send_message(MessageType.EXTENSION, bytes([EXT_HANDSHAKE_ID]) + payload)
        self.ext_handshake_send = True
        logger.debug(f"Sent extension handshake to {self.host}:{self.port}")

    async def send_interested(self) -> None:
        """
        发送Interested消息，表示对远端peer的数据感兴趣。
        Sends an Interested message, indicating interest in the remote peer's data.
        """
        if not self.ext_handshake_recv or self.interested_send or not self.ut_metadata_id: # 必须收到扩展握手并且对方支持ut_metadata
            return
        # 发送Interested消息
        await self.send_message(MessageType.INTERESTED)
        self.interested_send = True
        logger.debug(f"Sent Interested message to {self.host}:{self.port}")

    async def request_metadata_piece(self, piece_index: int) -> None:
        """
        请求指定的元数据块。
        Requests a specific metadata piece.
        """
        if not self.ut_metadata_id or not self.unchoke_recv: # 必须知道ut_metadata_id且对方已unchoke
            return
        # 构建请求元数据块的消息体
        # 消息类型 'request' (0), 请求的块索引
        payload = bencoder.encode({"msg_type": 0, "piece": piece_index})
        # 发送扩展消息，ut_metadata_id是远端peer告知的ID，payload是请求内容
        await self.send_message(MessageType.EXTENSION, bytes([self.ut_metadata_id]) + payload)
        logger.debug(f"Requested metadata piece {piece_index} from {self.host}:{self.port}")

    async def handle_message(self, msg_id: int, payload: bytes) -> None:
        """
        处理从远端peer接收到的消息。
        Handles messages received from the remote peer.
        """
        if msg_id == MessageType.CHOKE:
            logger.debug(f"Received CHOKE from {self.host}:{self.port}")
            self.unchoke_recv = False
        elif msg_id == MessageType.UNCHOKE:
            logger.debug(f"Received UNCHOKE from {self.host}:{self.port}")
            self.unchoke_recv = True
            if self.metadata_total_pieces > 0 and len(self.metadata_downloaded_pieces) < self.metadata_total_pieces:
                for i in range(self.metadata_total_pieces):
                    if i not in self.metadata_downloaded_pieces:
                        await self.request_metadata_piece(i)
                        break
        elif msg_id == MessageType.EXTENSION:
            # 收到扩展消息
            ext_msg_id = payload[0]
            ext_payload_raw = payload[1:] # 原始的bencoded数据或piece数据
            if ext_msg_id == EXT_HANDSHAKE_ID: # 扩展握手响应
                # 处理扩展握手响应
                self.ext_handshake_recv = True
                try:
                    decoded_payload = bencoder.decode(ext_payload_raw)
                    # 获取远端peer支持的ut_metadata ID
                    self.ut_metadata_id = get_ut_metadata(decoded_payload)
                    # 获取元数据总大小
                    self.metadata_size_val = get_metadata_size(decoded_payload)

                    if self.metadata_size_val > 0 and self.ut_metadata_id > 0:
                        # 计算元数据块总数
                        self.metadata_total_pieces = math.ceil(self.metadata_size_val / MAX_SIZE)
                        # 初始化元数据字节数组列表
                        self.metadata_data_list = [b""] * self.metadata_total_pieces
                        logger.debug(f"Ext handshake from {self.host}:{self.port}. ut_metadata_id: {self.ut_metadata_id}, metadata_size: {self.metadata_size_val}, total_pieces: {self.metadata_total_pieces}")
                        await self.send_interested()
                    else:
                        logger.warning(f"Peer {self.host}:{self.port} does not support metadata exchange (ut_metadata_id: {self.ut_metadata_id}) or metadata_size is invalid ({self.metadata_size_val}). Closing.")
                        await self.close()
                except Exception as e:
                    logger.error(f"Error decoding extension handshake from {self.host}:{self.port}: {e}", exc_info=True)
                    await self.close()
            elif ext_msg_id == self.ut_metadata_id and self.ut_metadata_id > 0 : # 元数据块响应，且我们知道ut_metadata_id
                try:
                    metadata_piece_offset = ext_payload_raw.find(b"ee") + 2
                    if metadata_piece_offset <= 1 :
                         logger.warning(f"Invalid metadata piece format from {self.host}:{self.port}: 'ee' not found or at beginning.")
                         await self.close()
                         return

                    decoded_header = bencoder.decode(ext_payload_raw[:metadata_piece_offset])
                    piece_index = decoded_header.get(b"piece", -1)
                    actual_metadata_block = ext_payload_raw[metadata_piece_offset:]

                    if not (0 <= piece_index < self.metadata_total_pieces):
                        logger.warning(f"Invalid piece index {piece_index} from {self.host}:{self.port}. Total pieces: {self.metadata_total_pieces}. Closing.")
                        await self.close()
                        return

                    expected_size = MAX_SIZE
                    if piece_index == self.metadata_total_pieces -1:
                        expected_size = self.metadata_size_val - (MAX_SIZE * piece_index)

                    if len(actual_metadata_block) != expected_size:
                        logger.warning(f"Metadata piece {piece_index} size mismatch for {self.host}:{self.port}. Expected {expected_size}, got {len(actual_metadata_block)}. Closing.")
                        await self.close()
                        return

                    if piece_index not in self.metadata_downloaded_pieces:
                        self.metadata_data_list[piece_index] = actual_metadata_block
                        self.metadata_downloaded_pieces.add(piece_index)
                        logger.debug(f"Received metadata piece {piece_index} from {self.host}:{self.port}. Downloaded {len(self.metadata_downloaded_pieces)}/{self.metadata_total_pieces}")

                        if len(self.metadata_downloaded_pieces) == self.metadata_total_pieces:
                            full_metadata = b"".join(self.metadata_data_list)
                            if hashlib.sha1(full_metadata).digest() == self.info_hash:
                                logger.info(f"Metadata for {binascii.hexlify(self.info_hash).decode()} successfully downloaded from {self.host}:{self.port}")
                                if self.on_metadata_complete_cb:
                                    self.on_metadata_complete_cb(full_metadata)
                                await self.close()
                            else:
                                logger.warning(f"Metadata hash mismatch for {binascii.hexlify(self.info_hash).decode()} from {self.host}:{self.port}. Corrupted data. Closing.")
                                self.metadata_downloaded_pieces.clear() # Allow retry if connection is re-established by chance
                                self.metadata_data_list = [b""] * self.metadata_total_pieces
                                await self.close()
                        elif self.unchoke_recv:
                            for i in range(self.metadata_total_pieces):
                                if i not in self.metadata_downloaded_pieces:
                                    await self.request_metadata_piece(i)
                                    break
                    else:
                        logger.debug(f"Already have piece {piece_index} from {self.host}:{self.port}, ignoring.")
                except Exception as e:
                    logger.error(f"Error processing metadata piece from {self.host}:{self.port}: {e}", exc_info=True)
                    await self.close()
        # 可以根据需要处理其他消息类型，如HAVE, BITFIELD, REQUEST, PIECE, CANCEL, PORT等

    async def close(self) -> None:
        """
        关闭与远端peer的连接。
        Closes the connection with the remote peer.
        """
        if self.closing:
            return
        self.closing = True
        logger.debug(f"Closing connection with {self.host}:{self.port} (closing flag: {self.closing})")
        if self.closing:
            return
        self.closing = True
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError, OSError, AttributeError) as e:
                logger.debug(f"Error during writer.wait_closed() for {self.host}:{self.port}: {e}")
            except Exception as e:
                logger.warning(f"Unexpected error during writer.wait_closed() for {self.host}:{self.port}: {e}", exc_info=True)

        self.writer = None
        self.reader = None
        logger.debug(f"Connection with {self.host}:{self.port} streams closed.")

        while True:
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                logger.warning(f"Unexpected error clearing queue for {self.host}:{self.port}: {e}", exc_info=True)
                break
        logger.debug(f"Message queue cleared for {self.host}:{self.port}.")


    async def start(self) -> None:
        """
        启动与peer的通信，包括握手、扩展握手和消息循环。
        如果成功获取元数据，会调用on_metadata_complete_cb回调。
        Starts communication with the peer, including handshake, extended handshake, and message loop.
        If metadata is successfully fetched, the on_metadata_complete_cb callback is called.
        """
        logger.debug(f"Starting client for {self.host}:{self.port} with info_hash {binascii.hexlify(self.info_hash).decode()}")
        if not await self.do_handshake():
            logger.info(f"Failed to handshake with {self.host}:{self.port}, closing connection.")
            # close() is called within do_handshake on failure.
            return

        await self.send_ext_handshake()

        try:
            await asyncio.wait_for(self._message_loop(), timeout=60)
        except asyncio.TimeoutError:
            logger.info(f"Overall timeout for metadata exchange with {self.host}:{self.port}.")
        except Exception as e:
            logger.error(f"Unexpected error in message loop for {self.host}:{self.port}: {e}", exc_info=True)
        finally:
            logger.debug(f"Client for {self.host}:{self.port} start() method finishing, ensuring close.")
            await self.close()

    async def _message_loop(self):
        """
        内部消息处理循环。
        Internal message processing loop.
        """
        while not self.closing:
            if self.metadata_total_pieces > 0 and len(self.metadata_downloaded_pieces) == self.metadata_total_pieces:
                # 元数据已下载完成，可以退出循环了，start()中的 finally会close
                break
            try:
                # 从队列中获取消息，设置超时以避免永久阻塞
                msg_id, payload = await asyncio.wait_for(self.queue.get(), timeout=30)
                await self.handle_message(msg_id, payload)
                self.queue.task_done()
            except asyncio.TimeoutError:
                if self.closing: # If already closing, just break
                    logger.debug(f"Message loop for {self.host}:{self.port} timed out but already closing.")
                    break

                logger.debug(f"Timeout waiting for message from queue for {self.host}:{self.port}.")
                if self.handshake_recv:
                    if self.ext_handshake_send and not self.ext_handshake_recv and self.metadata_total_pieces == 0:
                        logger.warning(f"Timeout waiting for extension handshake response from {self.host}:{self.port}, closing.")
                        await self.close()
                    elif self.interested_send and not self.unchoke_recv and self.metadata_total_pieces > 0:
                        logger.warning(f"Timeout waiting for UNCHOKE from {self.host}:{self.port} after sending INTERESTED, closing.")
                        await self.close()
                    elif self.metadata_total_pieces > 0 and len(self.metadata_downloaded_pieces) < self.metadata_total_pieces:
                        logger.warning(f"Timeout waiting for metadata piece from {self.host}:{self.port}, closing.")
                        await self.close()
                    else:
                        logger.debug(f"Sending keep-alive to {self.host}:{self.port}.")
                        await self.send_message(MessageType.KEEP_ALIVE)
                elif not self.handshake_recv:
                    logger.warning(f"Timeout waiting for initial messages (pre-handshake) from {self.host}:{self.port}, closing.")
                    await self.close()
            except Exception as e:
                logger.error(f"Error in message loop for {self.host}:{self.port}: {e}", exc_info=True)
                await self.close() # Ensure close on unexpected error in loop
                break # Exit loop on error

async def get_metadata(info_hash: bytes, my_id: bytes, host: str, port: int, loop) -> bytes | None:
    """
    尝试从单个peer处获取Torrent的元数据。
    它会创建一个WirePeerClient实例，执行握手、扩展握手，并请求元数据块。
    如果成功获取，返回元数据内容(bytes)；否则返回None。

    Tries to fetch the Torrent's metadata from a single peer.
    It creates a WirePeerClient instance, performs handshake, extended handshake,
    and requests metadata pieces.
    Returns the metadata content (bytes) if successful, otherwise None.
    """
    metadata_container = [None] # 使用列表作为容器，以便在回调中修改其值

    def on_metadata_complete(metadata_content: bytes):
        """元数据下载完成时的回调函数"""
        metadata_container[0] = metadata_content

    client = WirePeerClient(info_hash, my_id, host, port, on_metadata_complete, loop)
    try:
        await client.start()
    except Exception as e:
        logger.error(f"Exception during get_metadata call for {host}:{port} with info_hash {binascii.hexlify(info_hash).decode()}: {e}", exc_info=True)
        # client.start() has its own finally block that calls close.
        # If the error occurs before or after client.start() itself, we ensure close here.
        if not client.closing: # Ensure close is only called if not already initiated
            await client.close()
    # No explicit finally client.close() here, as client.start() handles its own closure.
    # The above try/except is more for errors outside the client.start() main try/finally.
    return metadata_container[0]
