# -*- coding: utf-8 -*-
"""
MP4 H.264 关键帧提取器 (生产级重构版)

功能:
- 从一个 'moov' box 的字节数据中精确解析 MP4 文件结构，并提取 H.264 关键帧元数据。
- 采用健壮的启发式逻辑，以应对现实世界中不完全符合规范的 MP4 文件。
- 异步地从数据源读取关键帧数据包。
"""
import sys
import struct
import logging
from io import BytesIO
from collections import namedtuple
from typing import BinaryIO, Generator, Tuple, Optional, Any

# --- 配置日志 ---
log = logging.getLogger(__name__)

# --- 数据结构定义 ---
SampleInfo = namedtuple("SampleInfo", ["offset", "size", "is_keyframe", "index", "pts"])
Keyframe = namedtuple("Keyframe", ["index", "sample_index", "pts", "timescale"])


class H264KeyframeExtractor:
    """一个健壮的 MP4 H.264 关键帧提取器。"""

    def __init__(self, moov_data: bytes, reader: Any):
        """
        初始化提取器。
        :param moov_data: 包含 'moov' box 完整内容的字节串。
        :param reader: 一个异步的、类文件读取器 (例如 AsyncTorrentReader)，
                       必须实现 async seek() 和 async read()。
        """
        self.reader = reader
        self.moov_stream = BytesIO(moov_data)
        self.keyframes: list[Keyframe] = []
        self.samples: list[SampleInfo] = []
        self.extradata: Optional[bytes] = None
        self.mode: str = "unknown"
        self.nal_length_size: int = 4
        self.timescale: int = 1000

        try:
            self._parse_structure()
        except Exception as e:
            log.error(f"解析 'moov' box 时发生严重错误: {e}", exc_info=True)
            pass

    def _parse_boxes(self, stream: BinaryIO) -> Generator[Tuple[str, BytesIO], None, None]:
        """同步解析一个流中的所有 box。"""
        # This function is adapted from the original class, as it's a good generic parser.
        while True:
            current_offset = stream.tell()
            header_data = stream.read(8)
            if not header_data or len(header_data) < 8: break
            size, box_type_bytes = struct.unpack('>I4s', header_data)
            box_type = box_type_bytes.decode('ascii', 'ignore')
            log.debug(f"在偏移量 {current_offset} 处找到 Box '{box_type}'，大小为 {size}")
            header_size = 8
            if size == 1:
                size_64_data = stream.read(8)
                if not size_64_data: break
                size = struct.unpack('>Q', size_64_data)[0]
                header_size = 16
            elif size == 0:
                current_pos = stream.tell()
                stream.seek(0, 2)
                size = stream.tell() - (current_pos - header_size)
                stream.seek(current_pos)

            payload_size = size - header_size
            if payload_size < 0:
                log.warning(f"在 Box '{box_type}' 中检测到无效的 payload 大小 ({payload_size})，停止解析。")
                break

            payload_content = stream.read(payload_size)
            if len(payload_content) < payload_size:
                log.warning(f"无法读取 Box '{box_type}' 的完整 payload，停止解析。")
                break

            payload = BytesIO(payload_content)
            yield box_type, payload

    def _find_box_payload(self, stream: BinaryIO, box_path: list[str]) -> Optional[BytesIO]:
        """从流的当前位置开始，递归查找嵌套的 Box 并返回其 payload。"""
        # This function is adapted from the original class.
        if not box_path: return stream
        target_box, remaining_path = box_path[0], box_path[1:]
        for box_type, payload in self._parse_boxes(stream):
            if box_type == target_box:
                return self._find_box_payload(payload, remaining_path)
        return None

    def _parse_structure(self):
        """
        Parses the MP4 structure from the moov box to find the video track and build a sample map.
        This logic is adapted from the user-provided robust implementation.
        """
        stream = self.moov_stream
        log.info(f"开始解析 'moov' box。总大小: {len(stream.getbuffer())} 字节。")

        # The data passed to the extractor is the complete 'moov' atom (header + payload).
        # Some files might have a nested 'moov' atom. We need to unwrap it to get to the
        # payload that contains the 'trak' atoms.
        stream.seek(0)
        # Peek at the box type to see if we need to unwrap.
        size, box_type = struct.unpack('>I4s', stream.read(8))
        stream.seek(0) # Reset after peeking.

        if box_type == b'moov':
            log.info("检测到 'moov' box 容器，将解析其 payload。")
            # The stream *is* the moov box. We need to parse its payload.
            # _parse_boxes yields the payload as a new BytesIO stream.
            found_inner = False
            for b_type, payload in self._parse_boxes(stream):
                if b_type == 'moov':
                    stream = payload # The new stream to parse is the payload of the outer moov.
                    found_inner = True
                    break
            if not found_inner:
                # This should not happen if the peek was correct, but as a safeguard:
                raise ValueError("Could not extract payload from outer 'moov' box.")

        # By this point, `stream` should hold the actual content with trak/mvhd etc.
        moov_payload = stream
        moov_payload.seek(0)

        # 1. Find the video track ('trak')
        trak_payload = None
        for t_type, t_payload_iter in self._parse_boxes(moov_payload):
            if t_type == 'trak':
                # To check if it's a video track, we need to check its handler type.
                # The payload needs to be checked without consuming it for later use.
                current_pos = t_payload_iter.tell()
                hdlr_payload = self._find_box_payload(t_payload_iter, ['mdia', 'hdlr'])
                t_payload_iter.seek(current_pos) # Reset for the next check or final use

                if hdlr_payload:
                    hdlr_payload.seek(8) # Skip version, flags, component type
                    handler_type = hdlr_payload.read(4)
                    if handler_type == b'vide':
                        log.info("找到视频轨道 ('trak' with 'vide' handler)。")
                        trak_payload = t_payload_iter
                        break # Use the first video track found

        if not trak_payload:
            # For better debugging, let's log what we *did* find.
            all_children = []
            moov_payload.seek(0)
            for t, _ in self._parse_boxes(moov_payload):
                all_children.append(t)
            log.error(f"在 'moov' 的 payload 中未找到视频轨道。找到的 box: {all_children}")
            raise ValueError("在 'moov' Box 中未找到有效的视频轨道。")

        # 2. Get timescale from 'mdhd'
        mdhd_payload = self._find_box_payload(trak_payload, ['mdia', 'mdhd'])
        if mdhd_payload:
            version = struct.unpack('>B', mdhd_payload.read(1))[0]
            # According to spec, timescale is at offset 12 for version 0, 20 for version 1
            mdhd_payload.seek(12 if version == 0 else 20)
            self.timescale = struct.unpack('>I', mdhd_payload.read(4))[0]
        else:
            log.warning("未找到 'mdhd' box，将使用默认 timescale。")
        trak_payload.seek(0) # Reset trak payload stream

        # 3. Find the 'stbl' (Sample Table) box which contains all sample info
        stbl_payload = self._find_box_payload(trak_payload, ['mdia', 'minf', 'stbl'])
        if not stbl_payload:
            raise ValueError("在视频轨道中未找到 'stbl' Box。")

        # 4. Parse all tables within 'stbl'
        tables = {b_type: b_payload for b_type, b_payload in self._parse_boxes(stbl_payload)}

        # 5. Build the sample map and get decoder configuration
        self._build_sample_map_and_config(tables)

    def _build_sample_map_and_config(self, tables: dict):
        """
        Parses the stbl tables to get decoder config (extradata) and build the full sample map.
        This logic is heavily adapted from the user-provided robust implementation.
        """
        # --- 1. Get Decoder Configuration ('avc1'/'avc3' and 'avcC') ---
        stsd_payload = tables.get('stsd')
        if not stsd_payload: raise ValueError("在 'stbl' Box 中未找到 'stsd' Box。")
        stsd_payload.seek(8) # Skip version, flags, entry_count

        # Try to find 'avc1' first
        avc1_payload = self._find_box_payload(stsd_payload, ['avc1'])
        if avc1_payload:
            log.info("检测到 'avc1' 采样条目，正在检查 'avcC' Box...")
            # The 'avcC' box is a sub-box of 'avc1'.
            avc1_payload.seek(78) # Fixed offset to where sub-boxes like avcC start
            avcc_payload_stream = self._find_box_payload(avc1_payload, ['avcC'])

            if avcc_payload_stream and len(avcc_payload_stream.getvalue()) > 5:
                # Found 'avc1' with a valid 'avcC' -> out-of-band mode
                self.mode = 'avc1'
                avcc_payload = avcc_payload_stream.getvalue()
                self.extradata = avcc_payload
                self.nal_length_size = (avcc_payload[4] & 0x03) + 1
                log.info(f"找到有效的 'avcC' Box，将使用 '带外' 模式 (NALU 长度: {self.nal_length_size} 字节)。")
            else:
                # Found 'avc1' but 'avcC' is missing or invalid -> fallback to in-band
                self.mode = 'avc3'
                log.warning("'avc1' 条目缺少有效 'avcC' Box，将回退到 '带内' (Annex B) 模式。")
        else:
            # If 'avc1' not found, try 'avc3'
            stsd_payload.seek(8) # Reset for fresh search
            avc3_payload = self._find_box_payload(stsd_payload, ['avc3'])
            if avc3_payload:
                self.mode = 'avc3'
                log.info("检测到 'avc3' 采样条目，将使用 '带内' (Annex B) 模式。")
            else:
                raise ValueError("在 'stsd' Box 中既未找到 'avc1' 也未找到 'avc3'。")

        # --- 2. Build Sample Map ---
        # Get sample sizes ('stsz')
        stsz_payload = tables.get('stsz'); stsz_payload.seek(4) # version, flags
        sample_size, sample_count = struct.unpack('>II', stsz_payload.read(8))
        sample_sizes = []
        if sample_size == 0:
            if sample_count > 0:
                sample_sizes = struct.unpack(f'>{sample_count}I', stsz_payload.read(sample_count * 4))

        # Get sync samples (keyframes) ('stss')
        stss_payload = tables.get('stss'); keyframe_set = set()
        if stss_payload:
            stss_payload.seek(4); entry_count = struct.unpack('>I', stss_payload.read(4))[0]
            if entry_count > 0:
                keyframe_set = set(struct.unpack(f'>{entry_count}I', stss_payload.read(entry_count * 4)))
        else: # If stss is missing, all samples are keyframes
            keyframe_set = set(range(1, sample_count + 1))

        # Get chunk offsets ('stco' or 'co64')
        co_box = tables.get('stco') or tables.get('co64')
        if not co_box: raise ValueError("未找到 'stco' 或 'co64' box。")
        co_payload = co_box; co_payload.seek(4)
        entry_count = struct.unpack('>I', co_payload.read(4))[0]
        unpack_char = '>I' if tables.get('stco') else '>Q'
        chunk_offsets = struct.unpack(f'>{entry_count}{unpack_char[-1]}', co_payload.read(entry_count * struct.calcsize(unpack_char)))

        # Get sample-to-chunk mappings ('stsc')
        stsc_payload = tables.get('stsc'); stsc_payload.seek(4)
        entry_count = struct.unpack('>I', stsc_payload.read(4))[0]
        stsc_entries = [struct.unpack('>III', stsc_payload.read(12)) for _ in range(entry_count)]

        # Get sample timestamps ('stts')
        stts_payload = tables.get('stts'); stts_payload.seek(4)
        entry_count = struct.unpack('>I', stts_payload.read(4))[0]
        stts_entries = [struct.unpack('>II', stts_payload.read(8)) for _ in range(entry_count)]

        # --- 3. Iterate and Create Full Sample List ---
        samples = []
        stsc_idx, sample_idx, current_time = 0, 0, 0
        stts_iter = iter(stts_entries)
        stts_count, stts_duration = next(stts_iter, (0, 0))
        stts_sample_idx_in_entry = 0

        for chunk_idx, chunk_offset in enumerate(chunk_offsets):
            # Find the correct stsc entry for this chunk
            if stsc_idx < len(stsc_entries) - 1 and (chunk_idx + 1) >= stsc_entries[stsc_idx + 1][0]:
                stsc_idx += 1
            first_chunk, samples_per_chunk, _ = stsc_entries[stsc_idx]

            current_offset_in_chunk = 0
            for _ in range(samples_per_chunk):
                if sample_idx >= sample_count: break

                # Calculate timestamp (pts) for this sample
                while stts_sample_idx_in_entry >= stts_count:
                    stts_sample_idx_in_entry -= stts_count
                    current_time += stts_count * stts_duration
                    stts_count, stts_duration = next(stts_iter, (sample_count, 0))
                pts = current_time + stts_sample_idx_in_entry * stts_duration
                stts_sample_idx_in_entry += 1

                size = sample_sizes[sample_idx] if sample_size == 0 else sample_size
                is_keyframe = (sample_idx + 1) in keyframe_set
                samples.append(SampleInfo(chunk_offset + current_offset_in_chunk, size, is_keyframe, sample_idx + 1, pts))
                current_offset_in_chunk += size
                sample_idx += 1

        self.samples = samples

        keyframe_samples = [s for s in self.samples if s.is_keyframe]
        self.keyframes = [
            Keyframe(i, s.index, s.pts, self.timescale)
            for i, s in enumerate(keyframe_samples)
        ]

        log.info(f"完成采样地图构建。共找到 {len(self.samples)} 个样本，其中 {len(self.keyframes)} 个是关键帧。")

    async def get_keyframe_packet(self, keyframe_index: int) -> Tuple[Optional[bytes], bytes]:
        """异步获取并处理一个关键帧的数据包。"""
        if not (0 <= keyframe_index < len(self.keyframes)):
            raise IndexError(f"关键帧索引 {keyframe_index} 越界。")

        target_sample_index = self.keyframes[keyframe_index].sample_index
        # The sample list is 0-indexed.
        target_sample = self.samples[target_sample_index - 1]

        log.info(f"正在提取第 {keyframe_index} 个关键帧: 样本索引={target_sample.index}, 偏移={target_sample.offset}, 大小={target_sample.size}")

        await self.reader.seek(target_sample.offset)
        sample_data = await self.reader.read(target_sample.size)

        if len(sample_data) != target_sample.size:
            raise IOError(f"读取关键帧 {keyframe_index} 的数据不完整。")

        if self.mode == 'avc1':
            # Convert sample from MP4 format (NALU length prefixed) to Annex B (start code prefixed)
            annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
            while cursor < len(sample_data):
                try:
                    nal_length = int.from_bytes(sample_data[cursor : cursor + self.nal_length_size], 'big')
                    cursor += self.nal_length_size
                    nal_data = sample_data[cursor : cursor + nal_length]
                    if len(nal_data) < nal_length:
                        log.warning("样本数据不完整，NALU 数据被截断。")
                        break
                    annexb_data.extend(start_code + nal_data)
                    cursor += nal_length
                except Exception:
                    log.error("解析 NALU 长度时出错，样本数据可能已损坏。")
                    break
            return self.extradata, bytes(annexb_data)
        else: # avc3 mode is already in Annex B format
            return None, sample_data
