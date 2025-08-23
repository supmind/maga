import asyncio
import os
import signal
import socket
import uvloop

uvloop.install()

from socket import inet_ntoa
from struct import unpack

import bencode2 as bencoder
import logging

from . import utils
from . import constants


__version__ = '3.0.0'


class Maga(asyncio.DatagramProtocol):
    def __init__(self, loop=None, bootstrap_nodes=constants.BOOTSTRAP_NODES, interval=1, handler=None):
        self.node_id = utils.random_node_id()
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()
        self.handler = handler or self._default_handler
        self.log = logging.getLogger("Crawler")

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

        if msg_type == constants.KRPC_RESPONSE:
            return self.handle_response(msg, addr=addr)

        if msg_type == constants.KRPC_QUERY:
            return asyncio.ensure_future(
                self.handle_query(msg, addr=addr), loop=self.loop
            )

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
        if constants.KRPC_R in msg:
            args = msg[constants.KRPC_R]
            if constants.KRPC_NODES in args:
                for node_id, ip, port in utils.split_nodes(args[constants.KRPC_NODES]):
                    self.ping(addr=(ip, port))

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
                self.handler(infohash, peer_addr),
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

    async def _default_handler(self, infohash, peer_addr):
        """
        Default handler for discovered infohashes. Does nothing.
        """
        pass