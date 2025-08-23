import asyncio
import os
import signal
import socket
import uvloop

uvloop.install()

from socket import inet_ntoa
from struct import unpack

import bencode2 as bencoder

from . import utils
from . import constants
from .node import Node
from .routing_table import RoutingTable


__version__ = '3.0.0'


class KRPCProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop=None):
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport.close()

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

        if msg_type == constants.KRPC_RESPONSE:
            return self.handle_response(msg, addr=addr)

        if msg_type == constants.KRPC_QUERY:
            return asyncio.ensure_future(
                self.handle_query(msg, addr=addr), loop=self.loop
            )

    def handle_response(self, msg, addr):
        pass

    async def handle_query(self, msg, addr):
        pass


class Maga(KRPCProtocol):
    def __init__(self, loop=None, bootstrap_nodes=constants.BOOTSTRAP_NODES, interval=1):
        super().__init__(loop)
        self.node_id = utils.random_node_id()
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
        self.loop.call_later(self.interval, self.loop.stop)

    def add_seen_node(self, node_id, ip, port):
        if len(node_id) != 20:
            return
        if ip == '0.0.0.0' or port == 0:
            return
        node = Node(node_id, ip, port)
        self.routing_table.add_node(node)

    async def auto_find_nodes(self):
        self.__running = True
        while self.__running:
            await asyncio.sleep(self.interval)
            for node in self.bootstrap_nodes:
                self.find_node(addr=node)

    def run(self, port=6881):
        coro = self.loop.create_datagram_endpoint(
                lambda: self, local_addr=('0.0.0.0', port)
        )
        transport, _ = self.loop.run_until_complete(coro)

        for signame in ('SIGINT', 'SIGTERM'):
            try:
                self.loop.add_signal_handler(getattr(signal, signame), self.stop)
            except NotImplementedError:
                # SIGINT and SIGTERM are not implemented on windows
                pass

        for node in self.bootstrap_nodes:
            # Bootstrap
            self.find_node(addr=node, node_id=self.node_id)

        asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
        self.loop.run_forever()
        self.loop.close()

    def handle_response(self, msg, addr):
        args = msg.get(constants.KRPC_R, {})
        sender_id = args.get(constants.KRPC_ID)
        if sender_id:
            self.add_seen_node(sender_id, addr[0], addr[1])

        if constants.KRPC_NODES in args:
            for node_id, ip, port in utils.split_nodes(args[constants.KRPC_NODES]):
                self.add_seen_node(node_id, ip, port)

    async def handle_query(self, msg, addr):
        args = msg[constants.KRPC_A]
        node_id = args[constants.KRPC_ID]
        self.add_seen_node(node_id, addr[0], addr[1])
        query_type = msg[constants.KRPC_Q]

        if query_type == constants.KRPC_PING:
            self.send_message({
                constants.KRPC_T: msg[constants.KRPC_T],
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id)
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
                    constants.KRPC_ID: self.fake_node_id(node_id),
                    constants.KRPC_NODES: nodes_bytes
                }
            }, addr=addr)
        elif query_type == constants.KRPC_GET_PEERS:
            infohash = args[constants.KRPC_INFO_HASH]
            # For now, we are just a crawler, not a full client.
            # We don't store peer information, so we can't return peers.
            # Instead, we return the closest nodes we know about.
            closest_nodes = self.routing_table.find_closest_nodes(infohash)
            nodes_bytes = b"".join([node.to_bytes() for node in closest_nodes])
            # The token is required for announce_peer. This is a dummy token.
            token = infohash[:4]
            self.send_message({
                constants.KRPC_T: msg[constants.KRPC_T],
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id),
                    constants.KRPC_NODES: nodes_bytes,
                    constants.KRPC_TOKEN: token
                }
            }, addr=addr)
            await self.handle_get_peers(infohash, addr)
        elif query_type == constants.KRPC_ANNOUNCE_PEER:
            # We are a crawler, so we are interested in the infohash.
            # We don't need to store the peer, just acknowledge the announcement.
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
            await self.handle_announce_peer(
                utils.proper_infohash(infohash), addr, peer_addr
            )

    def ping(self, addr, node_id=None):
        self.send_message({
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_T: constants.KRPC_PING_TID,
            constants.KRPC_Q: constants.KRPC_PING,
            constants.KRPC_A: {
                constants.KRPC_ID: self.fake_node_id(node_id)
            }
        }, addr=addr)

    def connection_lost(self, exc):
        self.__running = False
        super().connection_lost(exc)

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

    async def handle_get_peers(self, infohash, addr):
        await self.handler(infohash, addr)

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        await self.handler(infohash, addr)

    async def handler(self, infohash, addr):
        pass