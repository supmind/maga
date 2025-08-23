import asyncio
import os
import socket
import signal
import logging

import bencode2 as bencoder

from . import utils
from . import constants
from .node import Node
from .routing_table import RoutingTable


class Lookup:
    def __init__(self, loop, dht_node, target_id):
        self.loop = loop
        self.dht_node = dht_node
        self.target_id = target_id
        self.log = dht_node.log

        self.future = asyncio.Future(loop=self.loop)
        self.queried_nodes = set()
        self.pending_nodes = set()

        self.nodes_to_query = dht_node.routing_table.find_closest_nodes(target_id)
        if not self.nodes_to_query:
            self.log.warning("Lookup starting with empty routing table, using bootstrap nodes.")
            # Fallback to bootstrap nodes if routing table is empty
            for ip, port in self.dht_node.bootstrap_nodes:
                # We don't have the node ID, but we can still query the address
                self.nodes_to_query.append(Node(None, ip, port))

    async def start(self):
        self.log.debug(f"Starting lookup. Initial nodes to query: {len(self.nodes_to_query)}")
        self._send_queries()
        try:
            self.log.debug("Waiting for lookup future...")
            return await asyncio.wait_for(self.future, timeout=10)
        except asyncio.TimeoutError:
            self.log.debug(f"Lookup for {self.target_id.hex()} timed out.")
            if not self.future.done():
                self.future.set_result(None)
            return None

    def _send_queries(self):
        for node in self.nodes_to_query:
            if len(self.pending_nodes) >= constants.K:
                break
            # For bootstrap nodes, node_id will be None, which is fine for the first query
            if node.node_id not in self.queried_nodes and node.node_id not in self.pending_nodes:
                self.dht_node._send_get_peers_to_node(node, self.target_id, self)
                if node.node_id:
                    self.pending_nodes.add(node.node_id)

    def handle_response(self, sender_id, response_args):
        self.pending_nodes.discard(sender_id)
        self.queried_nodes.add(sender_id)

        if b'values' in response_args:
            peers = []
            for peer_info in response_args[b'values']:
                try:
                    ip = socket.inet_ntoa(peer_info[:4])
                    port = int.from_bytes(peer_info[4:6], 'big')
                    peers.append((ip, port))
                except:
                    continue
            if peers and not self.future.done():
                self.log.info(f"Lookup successful: found {len(peers)} peers for {self.target_id.hex()}")
                self.future.set_result(peers)

        elif b'nodes' in response_args:
            self.log.debug("Lookup response contains more nodes, continuing search...")
            new_nodes = []
            for node_id, ip, port in utils.split_nodes(response_args[b'nodes']):
                new_nodes.append(Node(node_id, ip, port))

            self.nodes_to_query.extend(new_nodes)
            self.nodes_to_query.sort(key=lambda n: utils.get_distance(n.node_id, self.target_id))

            if not self.pending_nodes and not self.future.done():
                 if not any(node.node_id not in self.queried_nodes for node in self.nodes_to_query):
                    self.log.warning(f"Lookup for {self.target_id.hex()} exhausted all nodes.")
                    self.future.set_result(None)

            if not self.future.done():
                self._send_queries()


class DHTNode(asyncio.DatagramProtocol):
    def __init__(self, loop=None, bootstrap_nodes=constants.BOOTSTRAP_NODES, interval=5, port=6882):
        self.node_id = utils.random_node_id()
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()
        self.port = port
        self.log = logging.getLogger(f"DHTNode:{self.port}")
        self.routing_table = RoutingTable(self.node_id)
        self.pending_lookups = {}
        self.pending_pings = {}
        self.find_nodes_task = None

        resolved_bootstrap_nodes = []
        for host, port_num in bootstrap_nodes:
            try:
                ip = socket.gethostbyname(host)
                resolved_bootstrap_nodes.append((ip, port_num))
            except socket.gaierror:
                pass
        self.bootstrap_nodes = tuple(resolved_bootstrap_nodes)

        self.__running = False
        self.interval = interval

    def stop(self):
        self.__running = False
        if self.find_nodes_task:
            self.find_nodes_task.cancel()
        if self.transport:
            self.transport.close()
        self.log.info("DHT Node stopped.")

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
        except Exception:
            self.log.exception("Error handling message")

    def send_message(self, data, addr):
        data.setdefault(constants.KRPC_T, constants.KRPC_DEFAULT_TID)
        self.transport.sendto(bencoder.bencode(data), addr)

    def handle_message(self, msg, addr):
        msg_type = msg.get(constants.KRPC_Y, constants.KRPC_ERROR)

        if msg_type == constants.KRPC_ERROR:
            self.log.debug(f"Received error message from {addr}: {msg.get(constants.KRPC_E)}")
            return
        if msg_type == constants.KRPC_RESPONSE:
            return self.handle_response(msg, addr)
        if msg_type == constants.KRPC_QUERY:
            return asyncio.ensure_future(
                self.handle_query(msg, addr=addr), loop=self.loop
            )

    def handle_response(self, msg, addr):
        tid = msg.get(constants.KRPC_T)

        if tid in self.pending_pings:
            self.log.debug(f"Received PING response from {addr}")
            self.pending_pings[tid].set_result(True)
            return

        if tid in self.pending_lookups:
            lookup = self.pending_lookups[tid]
            sender_id = msg.get(constants.KRPC_R, {}).get(constants.KRPC_ID)
            if sender_id:
                self.log.debug(f"Received response for lookup from {addr}")
                asyncio.ensure_future(self.add_seen_node(sender_id, addr[0], addr[1]))
                lookup.handle_response(sender_id, msg.get(constants.KRPC_R, {}))
            if lookup.future.done():
                del self.pending_lookups[tid]
            return

        args = msg.get(constants.KRPC_R, {})
        sender_id = args.get(constants.KRPC_ID)
        if sender_id:
            asyncio.ensure_future(self.add_seen_node(sender_id, addr[0], addr[1]))

        if constants.KRPC_NODES in args:
            self.log.debug(f"Received {len(args[constants.KRPC_NODES]) // 26} nodes from {addr}")
            for node_id, ip, port in utils.split_nodes(args[constants.KRPC_NODES]):
                asyncio.ensure_future(self.add_seen_node(node_id, ip, port))

    async def get_peers(self, infohash):
        self.log.info(f"Starting peer lookup for infohash: {infohash.hex()}")
        lookup = Lookup(self.loop, self, infohash)
        return await lookup.start()

    def _send_get_peers_to_node(self, node, infohash, lookup):
        tid = os.urandom(2)
        while tid in self.pending_lookups:
            tid = os.urandom(2)
        self.pending_lookups[tid] = lookup

        self.log.debug(f"Sending get_peers for {infohash.hex()} to {node.ip}:{node.port}")
        self.send_message({
            constants.KRPC_T: tid,
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_Q: constants.KRPC_GET_PEERS,
            constants.KRPC_A: {
                constants.KRPC_ID: self.node_id,
                constants.KRPC_INFO_HASH: infohash
            }
        }, (node.ip, node.port))

    async def handle_query(self, msg, addr):
        args = msg.get(constants.KRPC_A, {})
        node_id = args.get(constants.KRPC_ID)
        query_type = msg.get(constants.KRPC_Q)

        if not all([node_id, query_type]):
            return

        asyncio.ensure_future(self.add_seen_node(node_id, addr[0], addr[1]))

        if query_type == constants.KRPC_PING:
            self.send_message({
                constants.KRPC_T: msg[constants.KRPC_T],
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.node_id
                }
            }, addr=addr)
        elif query_type == constants.KRPC_FIND_NODE:
            target_id = args[constants.KRPC_TARGET]
            closest_nodes = self.routing_table.find_closest_nodes(target_id)
            nodes_bytes = b"".join([node.to_bytes() for node in closest_nodes])
            self.send_message({
                constants.KRPC_T: msg[constants.KRPC_T],
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.node_id,
                    constants.KRPC_NODES: nodes_bytes
                }
            }, addr=addr)
        elif query_type == constants.KRPC_GET_PEERS:
            infohash = args[constants.KRPC_INFO_HASH]
            closest_nodes = self.routing_table.find_closest_nodes(infohash)
            nodes_bytes = b"".join([node.to_bytes() for node in closest_nodes])
            token = infohash[:4]
            self.send_message({
                constants.KRPC_T: msg[constants.KRPC_T],
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.node_id,
                    constants.KRPC_NODES: nodes_bytes,
                    constants.KRPC_TOKEN: token
                }
            }, addr=addr)
        elif query_type == constants.KRPC_ANNOUNCE_PEER:
            tid = msg[constants.KRPC_T]
            self.send_message({
                constants.KRPC_T: tid,
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.node_id
                }
            }, addr=addr)

    async def add_seen_node(self, node_id, ip, port):
        if len(node_id) != 20 or ip == '0.0.0.0' or port == 0 or node_id == self.node_id:
            return

        node = Node(node_id, ip, port)
        status, oldest_node = self.routing_table.add_node(node)

        if status == "ADDED":
            self.log.debug(f"Added new node {node_id.hex()}. "
                           f"Table size: {len(self.routing_table.get_all_nodes())}")
        elif status == "UPDATED":
            self.log.debug(f"Updated node {node_id.hex()}.")
        elif status == "SPLIT":
            self.log.debug("Routing table bucket was split. Re-adding node.")
            await self.add_seen_node(node.node_id, node.ip, node.port)
        elif status == "FULL":
            self.log.debug(f"Bucket is full. Pinging oldest node {oldest_node.node_id.hex()}...")
            if await self.ping(oldest_node):
                self.log.debug(f"Oldest node {oldest_node.node_id.hex()} responded. Discarding new node.")
                await self.add_seen_node(oldest_node.node_id, oldest_node.ip, oldest_node.port)
            else:
                self.log.info(f"Oldest node {oldest_node.node_id.hex()} did not respond. Replacing it.")
                self.routing_table.remove_node(oldest_node)
                await self.add_seen_node(node.node_id, node.ip, node.port)

    async def ping(self, node, timeout=5):
        tid = os.urandom(2)
        while tid in self.pending_pings:
            tid = os.urandom(2)

        future = asyncio.Future(loop=self.loop)
        self.pending_pings[tid] = future

        self.send_message({
            constants.KRPC_T: tid,
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_Q: constants.KRPC_PING,
            constants.KRPC_A: { constants.KRPC_ID: self.node_id }
        }, (node.ip, node.port))

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            return False
        finally:
            self.pending_pings.pop(tid, None)

    def find_node(self, addr, target=None):
        if not target:
            target = utils.random_node_id()
        self.send_message({
            constants.KRPC_T: constants.KRPC_FIND_NODE_TID,
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_Q: constants.KRPC_FIND_NODE,
            constants.KRPC_A: {
                constants.KRPC_ID: self.node_id,
                constants.KRPC_TARGET: target
            }
        }, addr=addr)

    async def auto_find_nodes(self):
        self.__running = True
        while self.__running:
            # Find a random node to keep the table fresh
            self.find_node(
                self.bootstrap_nodes[0], target=utils.random_node_id()
            )
            await asyncio.sleep(self.interval)

    async def run(self):
        await self.loop.create_datagram_endpoint(
                lambda: self, local_addr=('0.0.0.0', self.port)
        )
        self.log.info(f"DHT Node listening on port {self.port}")

        # Bootstrap
        self.log.info(f"Bootstrapping with {len(self.bootstrap_nodes)} nodes...")
        for node in self.bootstrap_nodes:
            self.find_node(addr=node)

        self.find_nodes_task = asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
