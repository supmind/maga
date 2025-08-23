from collections import OrderedDict

from . import constants
from .utils import get_distance


class KBucket:
    def __init__(self, min_id, max_id):
        self.min_id = min_id
        self.max_id = max_id
        self.nodes = OrderedDict()

    def add_node(self, node):
        self.nodes[node.node_id] = node

    def remove_node(self, node):
        if node.node_id in self.nodes:
            del self.nodes[node.node_id]

    def get_node(self, node_id):
        return self.nodes.get(node_id)

    def get_all_nodes(self):
        return list(self.nodes.values())

    def __contains__(self, node_id):
        return self.min_id <= int.from_bytes(node_id, 'big') < self.max_id

    def __len__(self):
        return len(self.nodes)

    def __repr__(self):
        return f"<KBucket(min={self.min_id}, max={self.max_id})>"


class RoutingTable:
    def __init__(self, node_id):
        self.node_id = node_id
        self.buckets = [
            KBucket(constants.MIN_NODE_ID, constants.MAX_NODE_ID)
        ]

    def get_bucket_for_node(self, node_id):
        for bucket in self.buckets:
            if node_id in bucket:
                return bucket
        return None

    def add_node(self, node):
        if node.node_id == self.node_id:
            return

        bucket = self.get_bucket_for_node(node.node_id)
        if bucket.get_node(node.node_id):
            bucket.remove_node(node)
            bucket.add_node(node)
            return

        if len(bucket) < constants.K:
            bucket.add_node(node)
            return

        if self.node_id in bucket:
            self.split_bucket(bucket)
            self.add_node(node)

    def split_bucket(self, bucket):
        split_point = bucket.min_id + (bucket.max_id - bucket.min_id) // 2

        new_bucket = KBucket(split_point, bucket.max_id)
        bucket.max_id = split_point

        idx = self.buckets.index(bucket)
        self.buckets.insert(idx + 1, new_bucket)

        nodes_to_move = [
            node for node in bucket.get_all_nodes() if int.from_bytes(node.node_id, 'big') >= split_point
        ]

        for node in nodes_to_move:
            bucket.remove_node(node)
            new_bucket.add_node(node)

    def find_closest_nodes(self, target_id, count=constants.K):
        nodes = []
        for bucket in self.buckets:
            nodes.extend(bucket.get_all_nodes())

        nodes.sort(key=lambda node: get_distance(node.node_id, target_id))

        return nodes[:count]

    def get_all_nodes(self):
        nodes = []
        for bucket in self.buckets:
            nodes.extend(bucket.get_all_nodes())
        return nodes
