import pytest
from maga import utils

def test_split_peers():
    # Test with a valid peer string for two peers
    # Peer 1: 127.0.0.1:6881 -> b'\x7f\x00\x00\x01\x1a\xe1'
    # Peer 2: 8.8.8.8:51413 -> b'\x08\x08\x08\x08\xc8\xd5'
    peers_data = b'\x7f\x00\x00\x01\x1a\xe1\x08\x08\x08\x08\xc8\xd5'
    expected_peers = [("127.0.0.1", 6881), ("8.8.8.8", 51413)]
    result = list(utils.split_peers(peers_data))
    assert result == expected_peers

    # Test with an empty string
    peers_data_empty = b''
    result_empty = list(utils.split_peers(peers_data_empty))
    assert result_empty == []

    # Test with a malformed string (not a multiple of 6)
    peers_data_malformed = b'\x7f\x00\x00\x01\x1a'
    result_malformed = list(utils.split_peers(peers_data_malformed))
    assert result_malformed == []

def test_split_nodes():
    # Test with a valid node string for one node
    # Node ID: 20 bytes of 'a'
    # IP: 192.168.1.1 -> b'\xc0\xa8\x01\x01'
    # Port: 6881 -> b'\x1a\xe1'
    node_id = b'a' * 20
    node_addr = b'\xc0\xa8\x01\x01\x1a\xe1'
    nodes_data = node_id + node_addr
    expected_nodes = [(node_id, "192.168.1.1", 6881)]
    result = list(utils.split_nodes(nodes_data))
    assert result == expected_nodes

    # Test with a string for two nodes
    node_id_2 = b'b' * 20
    node_addr_2 = b'\x0a\x0a\x0a\x0a\xff\xff'
    nodes_data_two = nodes_data + node_id_2 + node_addr_2
    expected_nodes_two = [
        (node_id, "192.168.1.1", 6881),
        (node_id_2, "10.10.10.10", 65535)
    ]
    result_two = list(utils.split_nodes(nodes_data_two))
    assert result_two == expected_nodes_two

    # Test with an empty string
    nodes_data_empty = b''
    result_empty = list(utils.split_nodes(nodes_data_empty))
    assert result_empty == []

    # Test with a malformed string (not a multiple of 26)
    nodes_data_malformed = b'a' * 25
    result_malformed = list(utils.split_nodes(nodes_data_malformed))
    assert result_malformed == []

def test_get_distance():
    id1 = b'\x00' * 20
    id2 = b'\x00' * 19 + b'\x01'
    assert utils.get_distance(id1, id2) == 1

    id3 = b'\xff' * 20
    id4 = b'\x00' * 20
    expected_distance = (2**160) - 1
    assert utils.get_distance(id3, id4) == expected_distance

    # Test distance is symmetric
    assert utils.get_distance(id1, id3) == utils.get_distance(id3, id1)
