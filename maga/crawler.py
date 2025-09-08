import asyncio
import os
import signal
import socket
import uvloop

uvloop.install()

from socket import inet_ntoa
from struct import unpack

from datetime import datetime, timezone
import random
import collections
import bencode2 as bencoder
import logging

from . import utils
from . import constants


__version__ = '3.0.0'

K = 8


class Maga(asyncio.DatagramProtocol):
    def __init__(self, loop=None, bootstrap_nodes=constants.BOOTSTRAP_NODES, interval=1, handler=None):
        self.node_id = utils.random_node_id()
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()
        if handler:
            self.handler = handler
        self.log = logging.getLogger("Crawler")
        self._pending_queries = {}
        self.k_buckets = [collections.deque(maxlen=K) for _ in range(160)]
        self.k_bucket_locks = [asyncio.Lock() for _ in range(160)]

        resolved_bootstrap_nodes = []
        for host, port in bootstrap_nodes:
            try:
                ip = socket.gethostbyname(host)
                addr = (ip, port)
                resolved_bootstrap_nodes.append(addr)
                # Bootstrap nodes don't have IDs, so we can't place them in k-buckets yet.
                # The _add_node logic will handle them when they communicate.
            except socket.gaierror:
                pass
        self.bootstrap_nodes = tuple(resolved_bootstrap_nodes)

        self.__running = False
        self.interval = interval
        self.find_nodes_task = None

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport.close()
        self.__running = False
        super().connection_lost(exc)

    def datagram_received(self, data, addr):
        try:
            msg = bencoder.bdecode(data)
        except:
            return
        try:
            self.handle_message(msg, addr)
        except Exception as e:
            self.send_message(data={
                constants.KRPC_T: msg.get(constants.KRPC_T),
                constants.KRPC_Y: constants.KRPC_ERROR,
                constants.KRPC_E: constants.KRPC_SERVER_ERROR
            }, addr=addr)
            raise e

    def send_message(self, data, addr):
        data.setdefault(constants.KRPC_T, constants.KRPC_DEFAULT_TID)
        self.transport.sendto(bencoder.bencode(data), addr)

    def handle_message(self, msg, addr):
        msg_type = msg.get(constants.KRPC_Y, constants.KRPC_ERROR)
        if msg_type == constants.KRPC_ERROR:
            return

        try:
            node_id = msg[constants.KRPC_A][constants.KRPC_ID]
            asyncio.ensure_future(self._add_node(node_id, addr), loop=self.loop)
        except KeyError:
            # This happens on response messages, where the node ID is not present
            # in the top-level 'a' dictionary. We handle this by finding the node
            # by its address and updating its last_seen time.
            for bucket in self.k_buckets:
                node = next((n for n in bucket if n["addr"] == addr), None)
                if node:
                    node["last_seen"] = datetime.now(timezone.utc)
                    bucket.remove(node)
                    bucket.append(node)
                    break

        if msg_type == constants.KRPC_RESPONSE:
            return self.handle_response(msg, addr)
        elif msg_type == constants.KRPC_QUERY:
            return asyncio.ensure_future(self.handle_query(msg, addr), loop=self.loop)

    def stop(self):
        self.__running = False
        if self.find_nodes_task:
            self.find_nodes_task.cancel()
        if self.transport:
            self.transport.close()

    async def auto_find_nodes(self):
        self.__running = True
        while self.__running:
            try:
                await asyncio.sleep(self.interval)
                for node in self.bootstrap_nodes:
                    self.find_node(addr=node)
            except Exception:
                self.log.exception("Error in Crawler auto_find_nodes loop")

    async def run(self, port=6881):
        _, _ = await self.loop.create_datagram_endpoint(
                lambda: self, local_addr=('0.0.0.0', port)
        )

        for node in self.bootstrap_nodes:
            # Bootstrap
            self.find_node(addr=node, node_id=self.node_id)

        self.find_nodes_task = asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)

    def handle_response(self, msg, addr):
        # A response from a node we know about is a good sign
        for bucket in self.k_buckets:
            node = next((n for n in bucket if n["addr"] == addr), None)
            if node:
                node["response_count"] += 1
                break

        tid = msg.get(constants.KRPC_T)
        if tid in self._pending_queries:
            # The future is just waiting for any valid response, not a specific one
            # The caller will be responsible for parsing the response
            future = self._pending_queries.pop(tid)
            future.set_result(msg)
            # Don't return here, we might want to process the response further

        # In addition to our own queries, we also learn about new nodes
        # from other nodes' responses to us.
        args = msg.get(constants.KRPC_R, {})
        if constants.KRPC_NODES in args:
            for node_id, ip, port in utils.split_nodes(args[constants.KRPC_NODES]):
                asyncio.ensure_future(self._add_node(node_id, (ip, port)), loop=self.loop)

    async def handle_query(self, msg, addr):
        args = msg.get(constants.KRPC_A, {})
        node_id = args.get(constants.KRPC_ID)
        query_type = msg.get(constants.KRPC_Q)

        if not all([node_id, query_type]):
            return

        if query_type == constants.KRPC_GET_PEERS:
            infohash = args[constants.KRPC_INFO_HASH]
            token = infohash[:2]
            self.send_message({
                constants.KRPC_T: msg[constants.KRPC_T],
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id),
                    constants.KRPC_NODES: "",
                    constants.KRPC_TOKEN: token
                }
            }, addr=addr)
            asyncio.ensure_future(
                self.handle_get_peers(infohash, addr), loop=self.loop
            )
        elif query_type == constants.KRPC_ANNOUNCE_PEER:
            infohash = args[constants.KRPC_INFO_HASH]
            tid = msg[constants.KRPC_T]
            self.send_message({
                constants.KRPC_T: tid,
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id)
                }
            }, addr=addr)

            if args.get(constants.KRPC_IMPLIED_PORT, 0) != 0:
                peer_port = addr[1]
            else:
                peer_port = args[constants.KRPC_PORT]
            peer_addr = (addr[0], peer_port)

            asyncio.ensure_future(
                self.handle_announce_peer(infohash, addr, peer_addr),
                loop=self.loop
            )
        elif query_type == constants.KRPC_FIND_NODE:
            tid = msg[constants.KRPC_T]
            self.send_message({
                constants.KRPC_T: tid,
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id),
                    constants.KRPC_NODES: ""
                }
            }, addr=addr)
        elif query_type == constants.KRPC_PING:
            self.send_message({
                constants.KRPC_T: msg[constants.KRPC_T],
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id)
                }
            }, addr=addr)

        self.find_node(addr=addr, node_id=node_id)

    def ping(self, addr, node_id=None):
        self.send_message({
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_T: constants.KRPC_PING_TID,
            constants.KRPC_Q: constants.KRPC_PING,
            constants.KRPC_A: {
                constants.KRPC_ID: self.fake_node_id(node_id)
            }
        }, addr=addr)

    def fake_node_id(self, node_id=None):
        if node_id:
            return node_id[:-1]+self.node_id[-1:]
        return self.node_id

    def find_node(self, addr, node_id=None, target=None):
        if not target:
            target = utils.random_node_id()
        self.send_message({
            constants.KRPC_T: constants.KRPC_FIND_NODE_TID,
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_Q: constants.KRPC_FIND_NODE,
            constants.KRPC_A: {
                constants.KRPC_ID: self.fake_node_id(node_id),
                constants.KRPC_TARGET: target
            }
        }, addr=addr)

    async def _send_query_and_wait(self, query_data, addr, timeout=2):
        """
        Sends a query to a specific address and waits for a response.
        """
        tid = os.urandom(2)
        query_data[constants.KRPC_T] = tid

        future = self.loop.create_future()
        self._pending_queries[tid] = future

        self.send_message(query_data, addr)

        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self._pending_queries.pop(tid, None)

    async def get_peers_recursive(self, infohash, max_hops=2):
        """
        Performs a recursive, multi-hop get_peers query.
        """
        peers = set()
        queried_nodes = set()

        # Start with the closest nodes from our own k-buckets
        bucket_index = self._get_bucket_index(infohash)
        nodes_to_query = [node["addr"] for node in self.k_buckets[bucket_index]]
        if not nodes_to_query:
            nodes_to_query = list(self.bootstrap_nodes)

        for hop in range(max_hops):
            query_data = {
                constants.KRPC_Y: constants.KRPC_QUERY,
                constants.KRPC_Q: constants.KRPC_GET_PEERS,
                constants.KRPC_A: {
                    constants.KRPC_ID: self.node_id,
                    constants.KRPC_INFO_HASH: infohash
                }
            }

            tasks = [self._send_query_and_wait(query_data, addr) for addr in nodes_to_query if addr not in queried_nodes]
            queried_nodes.update(nodes_to_query)

            if not tasks:
                break

            responses = await asyncio.gather(*tasks)

            new_nodes_found = []
            for msg in responses:
                if not msg:
                    continue

                args = msg.get(constants.KRPC_R, {})
                if constants.KRPC_VALUES in args:
                    for peer_data_string in args[constants.KRPC_VALUES]:
                        for peer in utils.split_peers(peer_data_string):
                            peers.add(peer)

                if constants.KRPC_NODES in args:
                    for node_id, ip, port in utils.split_nodes(args[constants.KRPC_NODES]):
                        new_nodes_found.append((ip, port))

            if peers:
                # If we found peers, we can stop searching
                break

            # Prepare for the next hop
            nodes_to_query = new_nodes_found

        return peers

    def _get_bucket_index(self, node_id):
        distance = utils.get_distance(self.node_id, node_id)
        if distance == 0:
            return 0
        return distance.bit_length() - 1

    async def _add_node(self, node_id, addr):
        bucket_index = self._get_bucket_index(node_id)
        lock = self.k_bucket_locks[bucket_index]

        async with lock:
            bucket = self.k_buckets[bucket_index]

            # Check if node already exists by ID
            existing_node = next((n for n in bucket if n["id"] == node_id), None)
            if existing_node:
                # It exists, move it to the end to mark it as most recently seen
                existing_node["last_seen"] = datetime.now(timezone.utc)
                bucket.remove(existing_node)
                bucket.append(existing_node)
                return

            # If bucket is not full, add the new node
            if len(bucket) < K:
                bucket.append({
                    "id": node_id,
                    "addr": addr,
                    "last_seen": datetime.now(timezone.utc),
                    "first_seen": datetime.now(timezone.utc),
                    "response_count": 0
                })
                return

            # Bucket is full, challenge the least-recently-seen node (at the front)
            lru_node = bucket[0]

            ping_query = {
                constants.KRPC_Y: constants.KRPC_QUERY,
                constants.KRPC_Q: constants.KRPC_PING,
                constants.KRPC_A: {
                    constants.KRPC_ID: self.node_id
                }
            }
            response = await self._send_query_and_wait(ping_query, lru_node["addr"], timeout=1)

            if response is None:
                # Node did not respond, evict it and add the new one
                bucket.popleft() # popleft is more efficient for deque
                bucket.append({
                    "id": node_id,
                    "addr": addr,
                    "last_seen": datetime.now(timezone.utc),
                    "first_seen": datetime.now(timezone.utc),
                    "response_count": 0
                })
            else:
                # Node responded, move it to the end and discard the new candidate
                bucket.remove(lru_node)
                lru_node["last_seen"] = datetime.now(timezone.utc)
                bucket.append(lru_node)

    async def handler(self, infohash, addr, peer_addr=None):
        """
        Default handler for discovered infohashes. Does nothing.
        """
        raise NotImplementedError

    async def handle_get_peers(self, infohash, addr):
        try:
            await self.handler(infohash, addr)
        except NotImplementedError:
            pass

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        try:
            await self.handler(infohash, addr, peer_addr=peer_addr)
        except NotImplementedError:
            pass