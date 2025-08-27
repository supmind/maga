# -*- coding: utf-8 -*-
"""
该模块包含 VideoFile 类，负责处理与视频相关的操作，
如查找关键帧和下载帧数据。它还包含 AsyncTorrentReader 类。
"""
import asyncio
import io
import logging
import struct
from collections import OrderedDict
from typing import Optional

from .extractor import H264KeyframeExtractor, Keyframe


class AsyncTorrentReader:
    """
    一个用于 Torrent 中文件的异步、类文件读取器。
    它通过向 TorrentClient 请求 piece 来按需读取数据。
    """
    def __init__(self, client, handle, file_index):
        self.client = client
        self.handle = handle
        self.ti = handle.torrent_file()
        self.file_index = file_index
        file_storage = self.ti.files()
        self.file_size = file_storage.file_size(self.file_index)
        self.pos = 0
        self.piece_cache = OrderedDict()
        self.PIECE_CACHE_SIZE = 32
        self.log = logging.getLogger("AsyncTorrentReader")

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET: self.pos = offset
        elif whence == io.SEEK_CUR: self.pos += offset
        elif whence == io.SEEK_END: self.pos = self.file_size + offset
        return self.pos

    def tell(self):
        return self.pos

    async def read(self, size=-1):
        if size == -1: size = self.file_size - self.pos
        size = min(size, self.file_size - self.pos)
        if size <= 0: return b''

        result_buffer = bytearray(size)
        buffer_offset, bytes_to_go, current_file_pos = 0, size, self.pos
        piece_size = self.ti.piece_length()

        while bytes_to_go > 0:
            req = self.ti.map_file(self.file_index, current_file_pos, 1)
            piece_index, piece_offset = req.piece, req.start
            read_len = min(bytes_to_go, piece_size - piece_offset)

            if piece_index in self.piece_cache:
                piece_data = self.piece_cache[piece_index]
                self.piece_cache.move_to_end(piece_index)
            else:
                try:
                    piece_data = await self.client.download_and_read_piece(self.handle, piece_index)
                except Exception as e:
                    self.log.error(f"读取 piece {piece_index} 时出错: {e}")
                    raise IOError(f"获取 piece {piece_index} 失败") from e
                self.piece_cache[piece_index] = piece_data
                if len(self.piece_cache) > self.PIECE_CACHE_SIZE:
                    self.piece_cache.popitem(last=False)

            if piece_data is None: raise IOError(f"读取 piece {piece_index} 失败：未收到数据。")
            chunk = piece_data[piece_offset : piece_offset + read_len]
            result_buffer[buffer_offset : buffer_offset + len(chunk)] = chunk
            buffer_offset += read_len
            bytes_to_go -= read_len
            current_file_pos += read_len

        self.pos += size
        return bytes(result_buffer)


class VideoFile:
    def __init__(self, client, handle):
        self.client = client
        self.handle = handle
        self.log = logging.getLogger("VideoFile")
        self.file_index = self._find_largest_video_file_index()
        self._extractor: Optional[H264KeyframeExtractor] = None

    def _find_largest_video_file_index(self):
        files = self.handle.torrent_file().files()
        video_file_index, max_size = -1, -1
        for i in range(files.num_files()):
            path = files.file_path(i).lower()
            if path.endswith((".mp4", ".mkv", ".avi")) and files.file_size(i) > max_size:
                max_size = files.file_size(i)
                video_file_index = i
        return video_file_index

    def create_reader(self):
        return AsyncTorrentReader(self.client, self.handle, self.file_index)

    async def _get_moov_data(self, reader: AsyncTorrentReader) -> Optional[bytes]:
        """从文件的开头和结尾探测 'moov' box 并返回其原始字节。"""
        PROBE_SIZE = 10 * 1024 * 1024
        file_size = reader.file_size

        async def find_moov_in_chunk(chunk: bytes, start_offset_in_file: int) -> Optional[bytes]:
            stream = io.BytesIO(chunk)
            while True:
                header_data = stream.read(8)
                if not header_data or len(header_data) < 8: break

                current_pos_in_chunk = stream.tell() - 8
                size, box_type_bytes = struct.unpack('>I4s', header_data)

                if box_type_bytes == b'moov':
                    self.log.info(f"在偏移量 {start_offset_in_file + current_pos_in_chunk} 处找到 'moov' box。")
                    if size > len(chunk) - current_pos_in_chunk:
                        self.log.warning("'moov' box 大小超出探测块范围，可能不完整。")
                        return None # Potentially incomplete
                    stream.seek(current_pos_in_chunk)
                    return stream.read(size)

                if size == 1: # 64-bit size
                    size_64_data = stream.read(8)
                    if not size_64_data: break
                    size = struct.unpack('>Q', size_64_data)[0]

                if size == 0: # To end of file
                    stream.seek(0, 2)
                    size = stream.tell() - current_pos_in_chunk
                    stream.seek(current_pos_in_chunk + 8)

                stream.seek(current_pos_in_chunk + size)
            return None

        # Probe at the beginning
        self.log.debug(f"正在文件的开头 {PROBE_SIZE} 字节中探测 'moov' box。")
        reader.seek(0)
        head_data = await reader.read(min(PROBE_SIZE, file_size))
        moov_data = await find_moov_in_chunk(head_data, 0)
        if moov_data:
            return moov_data

        # Probe at the end, using reverse search
        if file_size > PROBE_SIZE:
            seek_pos = max(0, file_size - PROBE_SIZE)
            self.log.debug(f"正在文件的末尾 {PROBE_SIZE} 字节中探测 'moov' box (从 {seek_pos} 开始)。")
            reader.seek(seek_pos)
            tail_data = await reader.read(PROBE_SIZE)

            # Reverse search for 'moov' box signature
            search_pos = len(tail_data)
            while search_pos >= 8:
                found_pos = tail_data.rfind(b'moov', 0, search_pos)
                if found_pos == -1:
                    self.log.debug("在当前块中通过反向搜索未找到 'moov' 签名。")
                    break

                size_pos = found_pos - 4
                if size_pos < 0:
                    search_pos = found_pos
                    continue

                try:
                    size = struct.unpack('>I', tail_data[size_pos:found_pos])[0]
                    if size >= 8 and (size_pos + size) <= len(tail_data):
                        self.log.info(f"在偏移量 {seek_pos + size_pos} 处通过反向搜索找到 'moov' box。")
                        return tail_data[size_pos : size_pos + size]
                except struct.error:
                    pass

                search_pos = found_pos

            # Fallback to original forward scan on the tail chunk if reverse search fails
            self.log.debug("反向搜索 'moov' 失败，回退到对尾部块的顺序扫描。")
            moov_data_fallback = await find_moov_in_chunk(tail_data, seek_pos)
            if moov_data_fallback:
                return moov_data_fallback

        self.log.error("在文件中未找到 'moov' box。")
        return None

    async def _get_extractor(self) -> Optional[H264KeyframeExtractor]:
        """获取并缓存 H264KeyframeExtractor 实例。"""
        if self._extractor:
            return self._extractor

        reader = self.create_reader()
        moov_data = await self._get_moov_data(reader)
        if not moov_data:
            return None

        # 创建一个新的 reader 实例以供提取器使用，以避免 seek 位置冲突
        extractor_reader = self.create_reader()
        self._extractor = H264KeyframeExtractor(moov_data, extractor_reader)
        if not self._extractor.keyframes:
            self.log.error("提取器初始化失败或视频中没有关键帧。")
            return None

        return self._extractor

    async def get_keyframes(self) -> list[Keyframe]:
        """从视频文件中提取并筛选用于截图的关键帧。"""
        extractor = await self._get_extractor()
        if not extractor:
            return []

        all_keyframes = extractor.keyframes
        if not all_keyframes:
            return []

        # 根据视频时长和设定的数量范围，对关键帧列表进行筛选
        MIN_SCREENSHOTS = 5
        MAX_SCREENSHOTS = 50
        TARGET_INTERVAL_SEC = 180

        last_sample = extractor.samples[-1]
        duration_sec = last_sample.pts / extractor.timescale if extractor.timescale > 0 else 0

        if duration_sec > 0:
            num_screenshots = int(duration_sec / TARGET_INTERVAL_SEC)
            num_screenshots = max(MIN_SCREENSHOTS, min(num_screenshots, MAX_SCREENSHOTS))
        else:
            num_screenshots = 20  # 对于未知时长的视频，使用默认值

        if len(all_keyframes) <= num_screenshots:
            return all_keyframes
        else:
            # 在视频的 10% 到 90% 区间内选择
            start_idx = int(len(all_keyframes) * 0.1)
            end_idx = int(len(all_keyframes) * 0.9)

            if start_idx >= end_idx:
                candidate_keyframes = all_keyframes[1:-1] if len(all_keyframes) > 2 else all_keyframes
            else:
                candidate_keyframes = all_keyframes[start_idx:end_idx]

            if not candidate_keyframes:
                return []

            if len(candidate_keyframes) <= num_screenshots:
                return candidate_keyframes
            else:
                indices = [int(i * len(candidate_keyframes) / num_screenshots) for i in range(num_screenshots)]
                return [candidate_keyframes[i] for i in sorted(list(set(indices)))]

    async def download_keyframe_data(self, keyframe_index: int):
        """
        异步下载并处理一个关键帧的数据包。
        :param keyframe_index: 关键帧在提取器关键帧列表中的索引。
        :return: 一个包含 (extradata, packet_data) 的元组，如果失败则返回 None。
        """
        extractor = await self._get_extractor()
        if not extractor:
            return None

        try:
            return await extractor.get_keyframe_packet(keyframe_index)
        except (IndexError, IOError) as e:
            self.log.error(f"下载关键帧索引 {keyframe_index} 失败: {e}")
            return None
