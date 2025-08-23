import time
import socket


class Node:
    def __init__(self, node_id, ip=None, port=None, last_seen=None):
        self.node_id = node_id
        self.ip = ip
        self.port = port
        self.last_seen = last_seen or time.time()

    def __repr__(self):
        return f"<Node(id={self.node_id.hex()})>"

    def __eq__(self, other):
        return self.node_id == other.node_id

    def __hash__(self):
        return hash(self.node_id)

    def to_bytes(self):
        """
        Packs the node object into a 26-byte string.
        """
        try:
            return self.node_id + socket.inet_aton(self.ip) + self.port.to_bytes(2, 'big')
        except (OSError, AttributeError):
            # Handle cases where IP is invalid or None
            return b""

    @classmethod
    def from_bytes(cls, data):
        """
        Unpacks a 26-byte string into a Node object.
        """
        node_id = data[:20]
        ip = socket.inet_ntoa(data[20:24])
        port = int.from_bytes(data[24:26], 'big')
        return cls(node_id, ip, port)
