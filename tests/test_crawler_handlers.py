import asyncio
import unittest
from unittest.mock import MagicMock, patch
from maga.crawler import Maga
from maga import constants

class TestCrawlerHandlers(unittest.TestCase):
    def test_handler_methods_are_called(self):
        class MyCrawler(Maga):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.handle_get_peers_mock = MagicMock()
                self.handle_announce_peer_mock = MagicMock()

            async def handle_get_peers(self, infohash, addr):
                self.handle_get_peers_mock(infohash, addr)

            async def handle_announce_peer(self, infohash, addr, peer_addr):
                self.handle_announce_peer_mock(infohash, addr, peer_addr)

        async def run_test():
            crawler = MyCrawler()
            addr = ("127.0.0.1", 6881)
            infohash = b"12345678901234567890"
            peer_addr = ("127.0.0.1", 6882)

            # Simulate a get_peers query
            get_peers_query = {
                constants.KRPC_T: b"aa",
                constants.KRPC_Y: constants.KRPC_QUERY,
                constants.KRPC_Q: constants.KRPC_GET_PEERS,
                constants.KRPC_A: {
                    constants.KRPC_ID: b"abcdefghij0123456789",
                    constants.KRPC_INFO_HASH: infohash,
                },
            }
            with patch.object(crawler, "send_message"):
                await crawler.handle_query(get_peers_query, addr)

            await asyncio.sleep(0) # Yield control to the event loop

            crawler.handle_get_peers_mock.assert_called_once_with(infohash, addr)

            # Simulate an announce_peer query
            announce_peer_query = {
                constants.KRPC_T: b"bb",
                constants.KRPC_Y: constants.KRPC_QUERY,
                constants.KRPC_Q: constants.KRPC_ANNOUNCE_PEER,
                constants.KRPC_A: {
                    constants.KRPC_ID: b"abcdefghij0123456789",
                    constants.KRPC_INFO_HASH: infohash,
                    constants.KRPC_PORT: peer_addr[1],
                    constants.KRPC_IMPLIED_PORT: 0,
                },
            }
            with patch.object(crawler, "send_message"):
                await crawler.handle_query(announce_peer_query, addr)

            await asyncio.sleep(0) # Yield control to the event loop

            crawler.handle_announce_peer_mock.assert_called_once_with(infohash, addr, peer_addr)

        asyncio.run(run_test())

if __name__ == "__main__":
    unittest.main()
