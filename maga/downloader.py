import hashlib
import asyncio
import binascii
import struct
import math

from fastbencode import bencode, bdecode
from . import utils


class MessageType:
    REQUEST = 0
    DATA = 1
    REJECT = 2


BT_PROTOCOL = "BitTorrent protocol"
BT_PROTOCOL_LEN = len(BT_PROTOCOL)
EXT_ID = 20
EXT_HANDSHAKE_ID = 0
EXT_HANDSHAKE_MESSAGE = bytes([EXT_ID, EXT_HANDSHAKE_ID]) + bencode({b"m": {b"ut_metadata": 1}})

BLOCK = math.pow(2, 14)
MAX_SIZE = BLOCK * 1000
BT_HEADER = b'\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x01'


def get_ut_metadata(data):
    ut_metadata = b"ut_metadata"
    index = data.index(ut_metadata)+len(ut_metadata) + 1
    data = data[index:]
    return int(data[:data.index(b'e')])


def get_metadata_size(data):
    metadata_size = b"metadata_size"
    start = data.index(metadata_size) + len(metadata_size) + 1
    data = data[start:]
    return int(data[:data.index(b"e")])


class WirePeerClient:
    def __init__(self, infohash, loop=None):
        if isinstance(infohash, str):
            infohash = binascii.unhexlify(infohash.upper())
        self.infohash = infohash
        self.peer_id = utils.random_node_id()
        self.loop = loop or asyncio.get_event_loop()

        self.writer = None
        self.reader = None

        self.ut_metadata = 0
        self.metadata_size = 0
        self.handshaked = False
        self.pieces_num = 0
        self.pieces_received_num = 0
        self.pieces = None

    async def connect(self, ip, port):
        self.reader, self.writer = await asyncio.open_connection(
            ip, port
        )

    async def close(self):
        try:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
        except:
            pass

    def check_handshake(self, data):
        if data[:20] != BT_HEADER[:20]:
            return False
        if data[28:48] != self.infohash:
            return False
        if data[25] & 0x10 != 0x10:
            return False
        return True

    def write_message(self, message):
        length = struct.pack(">I", len(message))
        self.writer.write(length + message)

    def request_piece(self, piece):
        msg = bytes([EXT_ID, self.ut_metadata]) + bencode({b"msg_type": 0, b"piece": piece})
        self.write_message(msg)

    async def pieces_complete(self):
        metainfo = b''.join(self.pieces)

        if len(metainfo) != self.metadata_size:
            return await self.close()

        infohash = hashlib.sha1(metainfo).hexdigest()
        if binascii.unhexlify(infohash.upper()) != self.infohash:
            return await self.close()

        return bdecode(metainfo)

    async def work(self):
        self.writer.write(BT_HEADER + self.infohash + self.peer_id)
        while True:
            if not self.handshaked:
                data = await self.reader.readexactly(68)
                if self.check_handshake(data):
                    self.handshaked = True
                    self.write_message(EXT_HANDSHAKE_MESSAGE)
                else:
                    return await self.close()

            total_message_length, msg_id = struct.unpack("!IB", await self.reader.readexactly(5))
            payload_length = total_message_length - 1
            payload = await self.reader.readexactly(payload_length)

            if msg_id != EXT_ID:
                continue
            extended_id, extend_payload = payload[0], payload[1:]
            if extended_id == 0 and not self.ut_metadata:
                try:
                    self.ut_metadata = get_ut_metadata(extend_payload)
                    self.metadata_size = get_metadata_size(extend_payload)
                    if self.metadata_size > MAX_SIZE:
                        # Peer is trying to send a file that is too large.
                        # This is a common attack vector.
                        return await self.close()
                except:
                    return await self.close()
                self.pieces_num = math.ceil(self.metadata_size / BLOCK)
                self.pieces = [False] * self.pieces_num
                self.request_piece(0)
                continue

            try:
                split_index = extend_payload.index(b"ee")+2
                info = bdecode(extend_payload[:split_index])
                if info[b'msg_type'] != MessageType.DATA:
                    return await self.close()
                if info[b'piece'] != self.pieces_received_num:
                    return await self.close()
                self.pieces[info[b'piece']] = extend_payload[split_index:]
            except:
                return await self.close()
            self.pieces_received_num += 1
            if self.pieces_received_num == self.pieces_num:
                return await self.pieces_complete()
            else:
                self.request_piece(self.pieces_received_num)


async def get_metadata(infohash, ip, port, loop=None, timeout=15):
    """
    Connects to a single peer and attempts to download the metadata.
    """
    client = WirePeerClient(infohash, loop=loop)
    try:
        # The connection itself should also be subject to a timeout.
        await asyncio.wait_for(client.connect(ip, port), timeout=timeout)
        # Use a timeout to prevent waiting forever on an unresponsive peer
        metadata = await asyncio.wait_for(client.work(), timeout=timeout)
        return metadata
    except Exception:
        # This will catch timeouts, connection errors, etc.
        return None
    finally:
        await client.close()
