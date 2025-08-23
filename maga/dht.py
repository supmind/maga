import asyncio
import socket
import signal

import bencode2 as bencoder

from . import utils
from . import constants
from .node import Node
from .routing_table import RoutingTable


class DHTNode(asyncio.DatagramProtocol):
    def __init__(self, loop=None, bootstrap_nodes=constants.BOOTSTRAP_NODES, interval=5):
        self.node_id = utils.random_node_id()
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()
        self.routing_table = RoutingTable(self.node_id)

        resolved_bootstrap_nodes = []
        for host, port in bootstrap_nodes:
            try:
                ip = socket.gethostbyname(host)
                resolved_bootstrap_nodes.append((ip, port))
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
            pass

    def send_message(self, data, addr):
        data.setdefault(constants.KRPC_T, constants.KRPC_DEFAULT_TID)
        self.transport.sendto(bencoder.bencode(data), addr)

    def handle_message(self, msg, addr):
        msg_type = msg.get(constants.KRPC_Y, constants.KRPC_ERROR)

        if msg_type == constants.KRPC_ERROR:
            return
        if msg_type == constants.KRPC_RESPONSE:
            return self.handle_response(msg, addr)
        if msg_type == constants.KRPC_QUERY:
            return asyncio.ensure_future(
                self.handle_query(msg, addr=addr), loop=self.loop
            )

    def handle_response(self, msg, addr):
        args = msg.get(constants.KRPC_R, {})
        sender_id = args.get(constants.KRPC_ID)
        if sender_id:
            self.add_seen_node(sender_id, addr[0], addr[1])

        if constants.KRPC_NODES in args:
            for node_id, ip, port in utils.split_nodes(args[constants.KRPC_NODES]):
                self.add_seen_node(node_id, ip, port)

    async def handle_query(self, msg, addr):
        args = msg.get(constants.KRPC_A, {})
        node_id = args.get(constants.KRPC_ID)
        query_type = msg.get(constants.KRPC_Q)

        if not all([node_id, query_type]):
            return

        self.add_seen_node(node_id, addr[0], addr[1])

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

    def add_seen_node(self, node_id, ip, port):
        if len(node_id) != 20:
            return
        if ip == '0.0.0.0' or port == 0:
            return
        node = Node(node_id, ip, port)
        self.routing_table.add_node(node)

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

    async def run(self, port=6881):
        await self.loop.create_datagram_endpoint(
                lambda: self, local_addr=('0.0.0.0', port)
        )

        for node in self.bootstrap_nodes:
            self.find_node(addr=node)

        self.find_nodes_task = asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
