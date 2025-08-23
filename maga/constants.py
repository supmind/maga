BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
)

# KRPC message dictionary keys
KRPC_Y = b"y"
KRPC_T = b"t"
KRPC_R = b"r"
KRPC_Q = b"q"
KRPC_A = b"a"
KRPC_E = b"e"

# KRPC message type values
KRPC_QUERY = b"q"
KRPC_RESPONSE = b"r"
KRPC_ERROR = b"e"

# KRPC query methods
KRPC_PING = b"ping"
KRPC_FIND_NODE = b"find_node"
KRPC_GET_PEERS = b"get_peers"
KRPC_ANNOUNCE_PEER = b"announce_peer"

# KRPC argument keys
KRPC_ID = b"id"
KRPC_NODES = b"nodes"
KRPC_INFO_HASH = b"info_hash"
KRPC_PORT = b"port"
KRPC_IMPLIED_PORT = b"implied_port"
KRPC_TOKEN = b"token"
KRPC_TARGET = b"target"

# Default transaction ID
KRPC_DEFAULT_TID = b"tt"
KRPC_FIND_NODE_TID = b"fn"
KRPC_PING_TID = "pg"

# Error message
KRPC_SERVER_ERROR = [202, "Server Error"]

# Kademlia constants
K = 8  # K-bucket size
MIN_NODE_ID = 0
MAX_NODE_ID = 2**160 - 1
