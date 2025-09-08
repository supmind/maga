import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, ANY

from maga.crawler import Maga, K
from maga import utils, constants

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio

@pytest.fixture
def loop():
    """Create and provide a new asyncio event loop for each test."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def crawler(loop):
    """Create a new instance of the Maga crawler for each test."""
    # We don't need bootstrap nodes for these unit tests
    return Maga(loop=loop, bootstrap_nodes=[])

async def test_add_node_simple(crawler):
    """Test adding a node to a non-full k-bucket."""
    node_id = utils.random_node_id()
    addr = ("1.2.3.4", 1234)

    await crawler._add_node(node_id, addr)

    bucket_index = crawler._get_bucket_index(node_id)
    bucket = crawler.k_buckets[bucket_index]

    assert len(bucket) == 1
    assert bucket[0]["id"] == node_id
    assert bucket[0]["addr"] == addr

async def test_add_node_updates_existing(crawler):
    """Test that adding an existing node just moves it to the end (updates last_seen)."""
    node_id = utils.random_node_id()
    addr = ("1.2.3.4", 1234)

    # Add the node once
    await crawler._add_node(node_id, addr)
    bucket_index = crawler._get_bucket_index(node_id)
    bucket = crawler.k_buckets[bucket_index]
    first_node_entry = bucket[0]

    # Add it again
    await crawler._add_node(node_id, addr)

    assert len(bucket) == 1
    assert bucket[0] == first_node_entry

async def test_add_node_full_bucket_evicts_on_ping_failure(crawler):
    """
    Test the eviction logic: when a bucket is full, the oldest node is challenged.
    If it fails the ping, it gets evicted and the new node is added.
    """
    bucket_index = 10
    my_id_int = int.from_bytes(crawler.node_id, 'big')

    # Fill the bucket with K nodes that are guaranteed to be in this bucket
    for i in range(K):
        distance = (1 << bucket_index) + i
        node_id_int = my_id_int ^ distance
        node_id = node_id_int.to_bytes(20, 'big')
        await crawler._add_node(node_id, (f"1.1.1.{i}", 1111))

    assert len(crawler.k_buckets[bucket_index]) == K
    oldest_node = crawler.k_buckets[bucket_index][0]

    crawler._send_query_and_wait = AsyncMock(return_value=None)

    # A new node that should also be in this bucket
    new_distance = (1 << bucket_index) + K
    new_node_id_int = my_id_int ^ new_distance
    new_node_id = new_node_id_int.to_bytes(20, 'big')
    await crawler._add_node(new_node_id, (f"2.2.2.2", 2222))

    crawler._send_query_and_wait.assert_called_once_with(
            ANY, oldest_node["addr"], timeout=1
    )

    bucket = crawler.k_buckets[bucket_index]
    assert len(bucket) == K
    assert oldest_node not in bucket
    assert any(n["id"] == new_node_id for n in bucket)

async def test_add_node_full_bucket_keeps_on_ping_success(crawler):
    """
    Test the eviction logic: when a bucket is full, the oldest node is challenged.
    If it succeeds the ping, it is kept and the new node is discarded.
    """
    bucket_index = 10
    my_id_int = int.from_bytes(crawler.node_id, 'big')

    # Fill the bucket
    for i in range(K):
        distance = (1 << bucket_index) + i
        node_id_int = my_id_int ^ distance
        node_id = node_id_int.to_bytes(20, 'big')
        await crawler._add_node(node_id, (f"1.1.1.{i}", 1111))

    assert len(crawler.k_buckets[bucket_index]) == K
    oldest_node_before_ping = crawler.k_buckets[bucket_index][0]

    crawler._send_query_and_wait = AsyncMock(return_value={constants.KRPC_Y: constants.KRPC_RESPONSE})

    # A new node that should also be in this bucket
    new_distance = (1 << bucket_index) + K
    new_node_id_int = my_id_int ^ new_distance
    new_node_id = new_node_id_int.to_bytes(20, 'big')
    await crawler._add_node(new_node_id, (f"2.2.2.2", 2222))

    crawler._send_query_and_wait.assert_called_once()

    bucket = crawler.k_buckets[bucket_index]
    assert len(bucket) == K
    assert not any(n["id"] == new_node_id for n in bucket)
    assert oldest_node_before_ping["id"] == bucket[-1]["id"]

async def test_get_peers_returns_empty_set_if_no_peers_found(crawler):
    """Test that it returns an empty set if no peers are found."""
    infohash = utils.random_node_id()

    # Simulate a response that contains other nodes but no peers
    response_no_peers = {
        constants.KRPC_Y: constants.KRPC_RESPONSE,
        constants.KRPC_R: {constants.KRPC_NODES: b""}
    }
    crawler._send_query_and_wait = AsyncMock(return_value=response_no_peers)

    # Add a node to the crawler's k-buckets so it has somewhere to start the query
    close_node_id = infohash[:-1] + bytes([infohash[-1] ^ 0x01])
    await crawler._add_node(close_node_id, ("1.2.3.4", 1234))

    peers = await crawler.get_peers_recursive(infohash)

    assert crawler._send_query_and_wait.call_count > 0
    assert peers == set()

async def test_get_peers_returns_peers_when_found(crawler):
    """Test that it returns a set of peer addresses when they are found."""
    infohash = utils.random_node_id()
    peer1_addr = ("127.0.0.1", 6881)
    peer2_addr = ("8.8.8.8", 51413)

    # Peer data for two peers, corresponding to the addresses above
    peers_data = b'\x7f\x00\x00\x01\x1a\xe1\x08\x08\x08\x08\xc8\xd5'
    response_with_peers = {
        constants.KRPC_Y: constants.KRPC_RESPONSE,
        constants.KRPC_R: {
            constants.KRPC_ID: utils.random_node_id(),
            constants.KRPC_TOKEN: b"tok",
            constants.KRPC_VALUES: [peers_data]
        }
    }
    crawler._send_query_and_wait = AsyncMock(return_value=response_with_peers)

    # Add a starting node
    start_node_id = infohash[:-1] + bytes([infohash[-1] ^ 0x01])
    await crawler._add_node(start_node_id, ("1.2.3.4", 1234))

    found_peers = await crawler.get_peers_recursive(infohash)

    assert found_peers == {peer1_addr, peer2_addr}
