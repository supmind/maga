import io
import os
import time

def log(infohash, message):
    """Helper for consistent logging."""
    print(f"[{infohash}][IO:{os.getpid()}] {message}")

class TorrentFileIO(io.RawIOBase):
    def __init__(self, infohash: str, metadata: dict, file_index: int, file_size: int, request_queue, result_dict):
        self.infohash = infohash.upper()
        self.metadata = metadata
        self.file_index = file_index
        self.file_size = file_size
        self.request_queue = request_queue
        self.result_dict = result_dict

        self.pos = 0

        try:
            self._piece_length = self.metadata[b'piece length']
            log(self.infohash, f"Initialized for file index {self.file_index} with size {self.file_size} and piece_length {self._piece_length}")
        except KeyError:
            raise IOError(f"Invalid metadata for {self.infohash}: 'piece length' not found.")

    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        log(self.infohash, f"seek({offset}, {whence}) called. Current pos: {self.pos}")
        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
        elif whence == io.SEEK_END:
            self.pos = self.file_size + offset
        log(self.infohash, f"New pos: {self.pos}")
        return self.pos

    def tell(self):
        return self.pos

    def read(self, size=-1):
        log(self.infohash, f"read({size}) called. Current pos: {self.pos}")
        if self.pos >= self.file_size:
            log(self.infohash, "Read at EOF, returning b''")
            return b''

        read_size = min(size if size != -1 else self.file_size, self.file_size - self.pos)
        if read_size <= 0:
            log(self.infohash, f"Calculated read size is {read_size}, returning b''")
            return b''

        start_offset = self.pos
        end_offset = start_offset + read_size

        start_piece, start_piece_offset = divmod(start_offset, self._piece_length)
        end_piece, _ = divmod(end_offset - 1, self._piece_length)

        pieces_needed = range(start_piece, end_piece + 1)

        for piece_index in pieces_needed:
            key = ('piece', self.infohash, piece_index)
            if key not in self.result_dict:
                log(self.infohash, f"Requesting piece {piece_index}")
                self.request_queue.put(('get_piece', (self.infohash, piece_index)))

        all_pieces_data = {}
        for piece_index in pieces_needed:
            data = self._wait_for_piece(piece_index, timeout=1800)
            if not data:
                log(self.infohash, f"FATAL: Timeout or error waiting for piece {piece_index}")
                return b''
            all_pieces_data[piece_index] = data

        full_chunk = b"".join(all_pieces_data[i] for i in sorted(all_pieces_data.keys()))

        final_start = start_piece_offset
        final_end = final_start + read_size

        if len(full_chunk) < final_end:
             log(self.infohash, f"FATAL: Stitched chunk is too small. "
                   f"Needed {final_end} bytes, got {len(full_chunk)}.")
             return b''

        result = full_chunk[final_start:final_end]

        self.pos += len(result)
        log(self.infohash, f"Successfully read {len(result)} bytes.")
        return result

    def _wait_for_piece(self, piece_index, timeout=60):
        start_time = time.time()
        key = ('piece', self.infohash, piece_index)

        log(self.infohash, f"Waiting for piece {piece_index}...")
        while time.time() - start_time < timeout:
            data = self.result_dict.get(key)
            if data is not None:
                log(self.infohash, f"Got piece {piece_index} ({len(data)} bytes).")
                del self.result_dict[key]
                return data
            time.sleep(0.2)
        return None

    def close(self):
        log(self.infohash, "close() called.")
        pass
