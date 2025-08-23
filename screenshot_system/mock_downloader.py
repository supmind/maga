import av.datasets

class MockDownloader:
    """
    A mock downloader that simulates downloading from a torrent.
    It reads from a local sample MP4 file instead of the network.
    """
    def __init__(self):
        print("MockDownloader: Initializing...")
        try:
            # Get the path to a sample video file provided by PyAV
            self.video_path = av.datasets.curated("pexels/time-lapse-video-of-night-sky-857195.mp4")
            with open(self.video_path, "rb") as f:
                self.video_data = f.read()
            self.file_size = len(self.video_data)
            print(f"MockDownloader: Loaded sample video '{self.video_path}' ({self.file_size} bytes).")
        except Exception as e:
            print(f"MockDownloader: Failed to load sample video data: {e}")
            self.video_data = None
            self.file_size = 0

    def get_file_info(self):
        """Returns mock file info."""
        if self.video_data:
            return {
                "path": self.video_path,
                "size": self.file_size
            }
        return None

    def download_byte_range(self, infohash: str, file_index: int, offset: int, size: int) -> bytes:
        """
        Simulates downloading a byte range by slicing the in-memory video data.
        The infohash and file_index are ignored as we only have one sample file.
        """
        if not self.video_data:
            return b''

        print(f"MockDownloader: 'Downloading' {size} bytes from offset {offset}")

        start = offset
        end = offset + size

        return self.video_data[start:end]

if __name__ == '__main__':
    # Example of how to use the MockDownloader
    mock_downloader = MockDownloader()
    if mock_downloader.get_file_info():
        # Get the first 1024 bytes of the sample file
        data = mock_downloader.download_byte_range("mock_hash", 0, 0, 1024)
        print(f"Successfully 'downloaded' {len(data)} bytes.")
        # print(data)
    else:
        print("MockDownloader could not be initialized.")
