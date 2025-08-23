import libtorrent as lt
import time
import datetime
import traceback

LOG_FILE = 'downloader_service.log'

def log_to_file(message):
    """Appends a message to the service log file."""
    with open(LOG_FILE, 'a') as f:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        f.write(f"{timestamp} - {message}\n")

class DownloaderService:
    def __init__(self):
        # The queues are not needed for this test.
        pass

    def log(self, tag, message):
        """Logs a message to the dedicated service log file."""
        log_to_file(f"[{tag}] {message}")

    def run(self):
        """The main event loop for the service."""
        try:
            version_info = lt.version
        except Exception as e:
            version_info = f"Error getting version: {e}\n{traceback.format_exc()}"

        self.log("VERSION_TEST", f"libtorrent version in service process: {version_info}")
        self.log("VERSION_TEST", "Service process test complete. Exiting.")
        # The process will exit after this function returns.
