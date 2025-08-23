import hashlib
import asyncio
import binascii
import struct
import math

import bencode2 as bencoder
from . import utils


class MessageType:
    REQUEST = 0
    DATA = 1
    REJECT = 2


BT_PROTOCOL = "BitTorrent protocol"
BT_PROTOCOL_LEN = len(BT_PROTOCOL)
EXT_ID = 20
EXT_HANDSHAKE_ID = 0
EXT_HANDSHAKE_MESSAGE = bytes([EXT_ID, EXT_HANDSHAKE_ID]) + bencoder.bencode({"m": {"ut_metadata": 1}})

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

    def close(self):
        try:
            self.writer.close()
        except:
            pass

    def check_handshake(self, data):
        # Check BT Protocol Prefix
        if data[:20] != BT_HEADER[:20]:
            return False
        # Check InfoHash
        if data[28:48] != self.infohash:
            return False
        # Check support metadata exchange
        if data[25] & 0x10 != 0x10:
            return False
        return True

    def write_message(self, message):
        length = struct.pack(">I", len(message))
        self.writer.write(length + message)

    def request_piece(self, piece):
        msg = bytes([EXT_ID, self.ut_metadata]) + bencoder.bencode({"msg_type": 0, "piece": piece})
        self.write_message(msg)

    def pieces_complete(self):
        metainfo = b''.join(self.pieces)

        if len(metainfo) != self.metadata_size:
            # Wrong size
            return self.close()

        infohash = hashlib.sha1(metainfo).hexdigest()
        if binascii.unhexlify(infohash.upper()) != self.infohash:
            # Wrong infohash
            return self.close()

        return bencoder.bdecode(metainfo)

    async def work(self):
        self.writer.write(BT_HEADER + self.infohash + self.peer_id)
        while True:
            if not self.handshaked:
                data = await self.reader.readexactly(68)
                if self.check_handshake(data):
                    self.handshaked = True
                    # Send EXT Handshake
                    self.write_message(EXT_HANDSHAKE_MESSAGE)
                else:
                    return self.close()

            total_message_length, msg_id = struct.unpack("!IB", await self.reader.readexactly(5))
            # Total message length contains message id length, remove it
            payload_length = total_message_length - 1
            payload = await self.reader.readexactly(payload_length)

            if msg_id != EXT_ID:
                continue
            extended_id, extend_payload = payload[0], payload[1:]
            if extended_id == 0 and not self.ut_metadata:
                # Extend handshake, receive ut_metadata and metadata_size
                try:
                    self.ut_metadata = get_ut_metadata(extend_payload)
                    self.metadata_size = get_metadata_size(extend_payload)
                except:
                    return self.close()
                self.pieces_num = math.ceil(self.metadata_size / BLOCK)
                self.pieces = [False] * self.pieces_num
                self.request_piece(0)
                continue

            try:
                split_index = extend_payload.index(b"ee")+2
                info = bencoder.bdecode(extend_payload[:split_index])
                if info[b'msg_type'] != MessageType.DATA:
                    return self.close()
                if info[b'piece'] != self.pieces_received_num:
                    return self.close()
                self.pieces[info[b'piece']] = extend_payload[split_index:]
            except:
                return self.close()
            self.pieces_received_num += 1
            if self.pieces_received_num == self.pieces_num:
                return self.pieces_complete()
            else:
                self.request_piece(self.pieces_received_num)


from .dht import DHTNode


class Downloader:
    def __init__(self, loop=None, dht_port=6882, num_workers=10):
        self.loop = loop or asyncio.get_event_loop()
        self.dht_node = DHTNode(loop=self.loop)
        self.dht_port = dht_port
        self.download_queue = asyncio.Queue()
        self.num_workers = num_workers
        self.workers = []
        self._running = False

    async def run(self):
        await self.dht_node.run(port=self.dht_port)
        self._running = True
        for _ in range(self.num_workers):
            worker_task = self.loop.create_task(self._worker())
            self.workers.append(worker_task)
        print(f"Downloader service started with {self.num_workers} workers on DHT port {self.dht_port}.")

    def stop(self):
        self._running = False
        self.dht_node.stop()
        for worker in self.workers:
            worker.cancel()
        print("Downloader service stopped.")

    async def submit(self, infohash):
        await self.download_queue.put(infohash)

    async def _worker(self):
        while self._running:
            try:
                infohash = await self.download_queue.get()
                infohash_hex = binascii.hexlify(infohash).decode()

                peers = await self.dht_node.get_peers(infohash)
                if not peers:
                    print(f"[Worker] Could not find peers for {infohash_hex}")
                    continue

                for ip, port in peers:
                    client = WirePeerClient(infohash, loop=self.loop)
                    try:
                        await client.connect(ip, port)
                        metadata = await asyncio.wait_for(client.work(), timeout=10)
                        if metadata:
                            info = metadata.get(b'info', {})
                            file_name = info.get(b'name', b'Unknown').decode(errors='ignore')
                            print(f"[Worker] SUCCESS: Downloaded metadata for '{file_name}' ({infohash_hex})")
                            break
                    except Exception:
                        continue
                    finally:
                        client.close()
                else:
                    print(f"[Worker] FAILURE: Could not download metadata from any peers for {infohash_hex}")

            except asyncio.CancelledError:
                break
            except Exception:
                pass
            finally:
                self.download_queue.task_done()
