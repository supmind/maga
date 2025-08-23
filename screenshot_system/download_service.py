import asyncio
from maga.downloader import TorrentDownloader

class DownloaderService:
    def __init__(self, request_queue, result_dict):
        self.request_queue = request_queue
        self.result_dict = result_dict
        self.downloader = None

    def run(self):
        # This will be called in a separate thread
        while True:
            req_type, payload = self.request_queue.get()

            if req_type == 'add_torrent':
                infohash, metadata, peers = payload
                # For now, we only support one peer
                peer_ip, peer_port = peers[0]
                self.downloader = TorrentDownloader(
                    infohash, peer_ip, peer_port, metadata, self.result_dict
                )
                asyncio.run(self.downloader.download())

            elif req_type == 'get_piece':
                infohash, piece_index = payload
                # The new downloader will download all pieces, so we don't need to do anything here
                pass
