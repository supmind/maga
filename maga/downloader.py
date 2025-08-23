import asyncio
import binascii
import struct
from . import utils

BT_PROTOCOL = "BitTorrent protocol"
BT_HEADER = b'\x13' + BT_PROTOCOL.encode('utf-8') + b'\x00\x00\x00\x00\x00\x00\x00\x00'

class MessageType:
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

BLOCK_SIZE = 16384  # 16KB

class TorrentDownloader:
    def __init__(self, infohash, peer_ip, peer_port, metadata, result_dict, loop=None):
        if isinstance(infohash, str):
            infohash = binascii.unhexlify(infohash.upper())
        self.infohash = infohash
        self.peer_id = utils.random_node_id()
        self.peer_ip = peer_ip
        self.peer_port = peer_port
        self.metadata = metadata
        self.result_dict = result_dict
        self.loop = loop or asyncio.get_event_loop()

        self.reader = None
        self.writer = None
        self.peer_has_pieces = None
        self.am_choking = True
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False

        self.piece_length = self.metadata[b'piece length']
        self.pieces = self.metadata[b'pieces']
        self.num_pieces = len(self.pieces) // 20
        self.downloaded_pieces = {} # piece_index -> piece_data

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.peer_ip, self.peer_port)

    def close(self):
        if self.writer:
            self.writer.close()

    async def do_handshake(self):
        self.writer.write(BT_HEADER + self.infohash + self.peer_id)
        data = await self.reader.readexactly(68)
        if data[:20] != BT_HEADER[:20]:
            raise Exception("Invalid BT protocol header")
        if data[28:48] != self.infohash:
            raise Exception("Invalid infohash")
        return True

    def send_message(self, msg_type, payload=b''):
        msg = struct.pack('!IB', 1 + len(payload), msg_type) + payload
        self.writer.write(msg)

    async def message_loop(self):
        while len(self.downloaded_pieces) < self.num_pieces:
            data = await self.reader.readexactly(4)
            msg_len = struct.unpack('!I', data)[0]

            if msg_len == 0:
                continue

            data = await self.reader.readexactly(msg_len)
            msg_id = data[0]
            payload = data[1:]

            if msg_id == MessageType.BITFIELD:
                self.peer_has_pieces = list(bin(int.from_bytes(payload, 'big'))[2:])
                self.send_message(MessageType.INTERESTED)
            elif msg_id == MessageType.UNCHOKE:
                self.peer_choking = False
                await self.request_pieces()
            elif msg_id == MessageType.PIECE:
                piece_index, block_offset = struct.unpack('!II', payload[:8])
                block_data = payload[8:]

                if piece_index not in self.downloaded_pieces:
                    self.downloaded_pieces[piece_index] = b''
                self.downloaded_pieces[piece_index] += block_data

                if len(self.downloaded_pieces[piece_index]) == self.piece_length:
                    key = ('piece', self.infohash.hex().upper(), piece_index)
                    self.result_dict[key] = self.downloaded_pieces[piece_index]
                    print(f"Got piece {piece_index}")

    async def request_pieces(self):
        for i in range(self.num_pieces):
            for block_offset in range(0, self.piece_length, BLOCK_SIZE):
                block_length = min(BLOCK_SIZE, self.piece_length - block_offset)
                payload = struct.pack('!III', i, block_offset, block_length)
                self.send_message(MessageType.REQUEST, payload)

    async def download(self):
        try:
            await self.connect()
            await self.do_handshake()
            await self.message_loop()
        finally:
            self.close()
