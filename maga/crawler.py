import asyncio
import os
import signal
import socket
import uvloop

uvloop.install()

from socket import inet_ntoa
from struct import unpack

from datetime import datetime
import random
import collections
import bencode2 as bencoder
import logging

from . import utils
from . import constants


__version__ = '3.0.0'

ROUTING_TABLE_MAX_SIZE = 1000


class Maga(asyncio.DatagramProtocol):
    def __init__(self, loop=None, bootstrap_nodes=constants.BOOTSTRAP_NODES, interval=1, handler=None):
        self.node_id = utils.random_node_id()
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()
        self.handler = handler or self._default_handler
        self.log = logging.getLogger("Crawler")
        self._pending_queries = {}
        self.routing_table = {}

        resolved_bootstrap_nodes = []
        now = datetime.utcnow()
        for host, port in bootstrap_nodes:
            try:
                ip = socket.gethostbyname(host)
                addr = (ip, port)
                resolved_bootstrap_nodes.append(addr)
                self.routing_table[addr] = {
                    "last_seen": now,
                    "first_seen": now,
                    "response_count": 0
                }
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

        # Add the node to our routing table or update its last_seen time
        asyncio.ensure_future(self._add_node(addr), loop=self.loop)

        # If we know the node, update its last_seen time
        if addr in self.routing_table:
            self.routing_table[addr]["last_seen"] = datetime.utcnow()

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
        if addr in self.routing_table:
            self.routing_table[addr]["response_count"] += 1

        tid = msg.get(constants.KRPC_T)
        if tid in self._pending_queries:
            # The future is just waiting for any valid response, not a specific one
            # The caller will be responsible for parsing the response
            future = self._pending_queries.pop(tid)
            future.set_result(msg)
            return

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

    async def get_peers_sample(self, infohash, sample_size=20):
        """
        Sends a get_peers query to a random sample of known nodes and
        counts the number of unique peers found.
        """
        table_keys = list(self.routing_table.keys())
        if len(table_keys) < sample_size:
            nodes_to_query = table_keys
        else:
            nodes_to_query = random.sample(table_keys, sample_size)

        query_data = {
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_Q: constants.KRPC_GET_PEERS,
            constants.KRPC_A: {
                constants.KRPC_ID: self.node_id,
                constants.KRPC_INFO_HASH: infohash
            }
        }

        tasks = [self._send_query_and_wait(query_data, addr) for addr in nodes_to_query]
        responses = await asyncio.gather(*tasks)

        peers = set()
        for msg in responses:
            if msg:
                args = msg.get(constants.KRPC_R, {})
                if constants.KRPC_VALUES in args:
                    for peer in utils.split_peers(args[constants.KRPC_VALUES]):
                        peers.add(peer)
        return len(peers)

    async def _add_node(self, addr):
        if addr in self.routing_table:
            return

        if len(self.routing_table) < ROUTING_TABLE_MAX_SIZE:
            self.routing_table[addr] = {
                "last_seen": datetime.utcnow(),
                "first_seen": datetime.utcnow(),
                "response_count": 0
            }
            return

        # Table is full, find the worst node to challenge
        # "Worst" is oldest last_seen and lowest response_count
        worst_node_addr = min(
            self.routing_table,
            key=lambda k: (self.routing_table[k]['last_seen'], self.routing_table[k]['response_count'])
        )

        # Challenge the worst node
        ping_query = {
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_Q: constants.KRPC_PING,
            constants.KRPC_A: {
                constants.KRPC_ID: self.node_id
            }
        }
        response = await self._send_query_and_wait(ping_query, worst_node_addr, timeout=1)

        if response is None:
            # Worst node did not respond, evict it and add the new one
            self.routing_table.pop(worst_node_addr)
            self.routing_table[addr] = {
                "last_seen": datetime.utcnow(),
                "first_seen": datetime.utcnow(),
                "response_count": 0
            }
        else:
            # Worst node responded, keep it (its last_seen is already updated)
            # and discard the new node candidate.
            pass

    async def _default_handler(self, infohash, peer_addr):
        """
        Default handler for discovered infohashes. Does nothing.
        """
        pass