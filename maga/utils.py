import binascii
import os
from socket import inet_ntoa
from struct import unpack


def proper_infohash(infohash):
    if isinstance(infohash, bytes):
        # Convert bytes to hex
        infohash = binascii.hexlify(infohash).decode('utf-8')
    return infohash.upper()


def random_node_id(size=20):
    return os.urandom(size)


def split_nodes(nodes):
    length = len(nodes)
    if (length % 26) != 0:
        return

    for i in range(0, length, 26):
        nid = nodes[i:i+20]
        ip = inet_ntoa(nodes[i+20:i+24])
        port = unpack("!H", nodes[i+24:i+26])[0]
        yield nid, ip, port


def get_distance(node1_id, node2_id):
    """
    Calculate the XOR distance between two node IDs.
    """
    return int.from_bytes(node1_id, 'big') ^ int.from_bytes(node2_id, 'big')
