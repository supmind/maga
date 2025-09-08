import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, ANY

from maga.crawler import Maga, K
from maga import utils

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
    node_id_prefix = crawler.node_id[:-1]
    bucket_index = crawler._get_bucket_index(node_id_prefix + b'\x01')

    for i in range(K):
        node_id = node_id_prefix + bytes([i])
        await crawler._add_node(node_id, (f"1.1.1.{i}", 1111))

    assert len(crawler.k_buckets[bucket_index]) == K
    oldest_node = crawler.k_buckets[bucket_index][0]

    crawler._send_query_and_wait = AsyncMock(return_value=None)

    new_node_id = node_id_prefix + bytes([K])
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
    node_id_prefix = crawler.node_id[:-1]
    bucket_index = crawler._get_bucket_index(node_id_prefix + b'\x01')

    for i in range(K):
        node_id = node_id_prefix + bytes([i])
        await crawler._add_node(node_id, (f"1.1.1.{i}", 1111))

    assert len(crawler.k_buckets[bucket_index]) == K
    oldest_node_before_ping = crawler.k_buckets[bucket_index][0]

    crawler._send_query_and_wait = AsyncMock(return_value={"y": "r"})

    new_node_id = node_id_prefix + bytes([K])
    await crawler._add_node(new_node_id, (f"2.2.2.2", 2222))

    crawler._send_query_and_wait.assert_called_once()

    bucket = crawler.k_buckets[bucket_index]
    assert len(bucket) == K
    assert not any(n["id"] == new_node_id for n in bucket)
    assert oldest_node_before_ping["id"] == bucket[-1]["id"]

async def test_get_peers_returns_zero_if_no_peers_found(crawler):
    """Test that it returns 0 if no peers are found after 2 hops."""
    infohash = utils.random_node_id()

    crawler._send_query_and_wait = AsyncMock(return_value={"y": "r", "r": {"nodes": b""}})

    close_node_id = infohash[:-1] + bytes([infohash[-1] ^ 0x01])
    await crawler._add_node(close_node_id, ("1.2.3.4", 1234))

    peer_count = await crawler.get_peers_recursive(infohash)

    assert crawler._send_query_and_wait.call_count > 0
    assert peer_count == 0
