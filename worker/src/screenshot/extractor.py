# -*- coding: utf-8 -*-
"""
MP4 关键帧提取器 (生产级重构版)

功能:
- 从一个 'moov' box 的字节数据中精确解析 MP4 文件结构，并提取 H.264, H.265 (HEVC), AV1 关键帧元数据。
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


class KeyframeExtractor:
    """一个健壮的 MP4 视频关键帧提取器，支持 H.264, H.265, AV1。"""

    def __init__(self, moov_data: Optional[bytes]):
        """
        初始化提取器。
        :param moov_data: 包含 'moov' box 完整内容的字节串, or None if restoring state.
        """
        if moov_data:
            self.moov_stream = BytesIO(moov_data)
        else:
            self.moov_stream = None

        self.keyframes: list[Keyframe] = []
        self.samples: list[SampleInfo] = []
        self.extradata: Optional[bytes] = None
        self.codec_name: Optional[str] = None
        self.mode: str = "unknown"
        self.nal_length_size: int = 4
        self.timescale: int = 1000

        if moov_data:
            try:
                self._parse_structure()
            except Exception as e:
                log.error("解析 'moov' box 时发生严重错误: %s", e, exc_info=True)
                # 让错误传播以便服务层处理
                raise

    def _parse_boxes(self, stream: BinaryIO) -> Generator[Tuple[str, BytesIO], None, None]:
        """同步解析一个流中的所有 box。"""
        while True:
            current_offset = stream.tell()
            header_data = stream.read(8)
            if not header_data or len(header_data) < 8: break
            size, box_type_bytes = struct.unpack('>I4s', header_data)
            box_type = box_type_bytes.decode('ascii', 'ignore')
            log.debug("在偏移量 %d 处找到 Box '%s'，大小为 %d", current_offset, box_type, size)
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
                log.warning("在 Box '%s' 中检测到无效的 payload 大小 (%d)，停止解析。", box_type, payload_size)
                break

            payload_content = stream.read(payload_size)
            if len(payload_content) < payload_size:
                log.warning("无法读取 Box '%s' 的完整 payload，停止解析。", box_type)
                break

            payload = BytesIO(payload_content)
            yield box_type, payload

    def _find_box_payload(self, stream: BinaryIO, box_path: list[str]) -> Optional[BytesIO]:
        """从流的当前位置开始，递归查找嵌套的 Box 并返回其 payload。"""
        if not box_path: return stream
        target_box, remaining_path = box_path[0], box_path[1:]
        for box_type, payload in self._parse_boxes(stream):
            if box_type == target_box:
                return self._find_box_payload(payload, remaining_path)
        return None

    def _parse_structure(self):
        """
        解析 'moov' box 的 MP4 结构，找到视频轨道并建立采样地图。
        """
        stream = self.moov_stream
        log.info("开始解析 'moov' box。总大小: %d 字节。", len(stream.getbuffer()))

        stream.seek(0)
        size, box_type = struct.unpack('>I4s', stream.read(8))
        stream.seek(0)

        if box_type == b'moov':
            log.info("检测到 'moov' box 容器，将解析其 payload。")
            found_inner = False
            for b_type, payload in self._parse_boxes(stream):
                if b_type == 'moov':
                    stream = payload
                    found_inner = True
                    break
            if not found_inner:
                raise ValueError("无法从外部 'moov' box 中提取 payload。")

        moov_payload = stream
        moov_payload.seek(0)

        # 1. 找到视频轨道 ('trak')
        trak_payload = None
        for t_type, t_payload_iter in self._parse_boxes(moov_payload):
            if t_type == 'trak':
                current_pos = t_payload_iter.tell()
                hdlr_payload = self._find_box_payload(t_payload_iter, ['mdia', 'hdlr'])
                t_payload_iter.seek(current_pos)

                if hdlr_payload:
                    hdlr_payload.seek(8)
                    handler_type = hdlr_payload.read(4)
                    if handler_type == b'vide':
                        log.info("找到视频轨道 ('trak' with 'vide' handler)。")
                        trak_payload = t_payload_iter
                        break

        if not trak_payload:
            all_children = [t for t, _ in self._parse_boxes(moov_payload)]
            log.error("在 'moov' 的 payload 中未找到视频轨道。找到的 box: %s", all_children)
            raise ValueError("在 'moov' Box 中未找到有效的视频轨道。")

        # 2. 从 'mdhd' 获取 timescale
        mdhd_payload = self._find_box_payload(trak_payload, ['mdia', 'mdhd'])
        if mdhd_payload:
            version = struct.unpack('>B', mdhd_payload.read(1))[0]
            mdhd_payload.seek(12 if version == 0 else 20)
            self.timescale = struct.unpack('>I', mdhd_payload.read(4))[0]
        else:
            log.warning("未找到 'mdhd' box，将使用默认 timescale。")
        trak_payload.seek(0)

        # 3. 找到 'stbl' (Sample Table) box
        stbl_payload = self._find_box_payload(trak_payload, ['mdia', 'minf', 'stbl'])
        if not stbl_payload:
            raise ValueError("在视频轨道中未找到 'stbl' Box。")

        # 4. 解析 'stbl' 内的所有表
        tables = {b_type: b_payload for b_type, b_payload in self._parse_boxes(stbl_payload)}

        # 5. 建立采样地图并获取解码器配置
        self._build_sample_map_and_config(tables)

    def _build_sample_map_and_config(self, tables: dict):
        """
        解析 stbl 表以获取解码器配置 (extradata) 并构建完整的采样地图。
        此方法经过重构以支持 H.264, H.265 (HEVC) 和 AV1。
        """
        stsd_payload = tables.get('stsd')
        if not stsd_payload: raise ValueError("在 'stbl' Box 中未找到 'stsd' Box。")
        stsd_payload.seek(8) # 跳过 version, flags, entry_count

        # 定义支持的编解码器及其属性
        codec_configs = {
            # H.264
            'avc1': {'codec': 'h264', 'config_box': 'avcC', 'out_band_mode': 'avc1', 'in_band_mode': 'avc3'},
            'avc3': {'codec': 'h264', 'config_box': None, 'in_band_mode': 'avc3'},
            # H.265 / HEVC
            'hvc1': {'codec': 'hevc', 'config_box': 'hvcC', 'out_band_mode': 'avc1', 'in_band_mode': 'avc3'},
            'hev1': {'codec': 'hevc', 'config_box': 'hvcC', 'out_band_mode': 'avc1', 'in_band_mode': 'avc3'},
            # AV1
            'av01': {'codec': 'av1', 'config_box': 'av1C', 'out_band_mode': 'av01', 'in_band_mode': None},
        }

        found_codec = False
        # 遍历 stsd 中的所有采样条目
        for sample_entry_type, sample_entry_payload in self._parse_boxes(stsd_payload):
            if sample_entry_type in codec_configs:
                log.info(f"检测到支持的采样条目 '{sample_entry_type}'。")
                config = codec_configs[sample_entry_type]
                self.codec_name = config['codec']
                config_box_name = config.get('config_box')

                # 寻找解码器配置 Box (例如 avcC, hvcC, av1C)
                if config_box_name:
                    sample_entry_payload.seek(78)  # 跳过通用的 VisualSampleEntry 头部
                    config_box_payload = None
                    try:
                        for box_type, payload in self._parse_boxes(sample_entry_payload):
                            if box_type == config_box_name:
                                config_box_payload = payload
                                break
                    except struct.error:
                        log.warning(f"解析 '{sample_entry_type}' 中的子 box 失败。文件可能不符合标准。")

                    if config_box_payload and len(config_box_payload.getvalue()) > 5:
                        # 找到有效的配置 box -> 带外模式
                        self.mode = config['out_band_mode']
                        config_data = config_box_payload.getvalue()
                        self.extradata = config_data
                        if self.codec_name in ['h264', 'hevc']:
                            if self.codec_name == 'h264':
                                # For avcC, the nal_length_size is in byte 4
                                self.nal_length_size = (config_data[4] & 0x03) + 1
                            else: # hevc
                                # For hvcC, the nal_length_size is in byte 21
                                self.nal_length_size = (config_data[21] & 0x03) + 1
                            log.info(f"找到有效的 '{config_box_name}' Box，将使用“带外”模式 (NALU 长度: {self.nal_length_size} 字节)。")
                        else: # AV1
                            log.info(f"找到有效的 '{config_box_name}' Box，将使用“带外”模式。")
                    else:
                        # 未找到有效配置 box -> 回退到带内模式 (如果支持)
                        if config.get('in_band_mode'):
                            self.mode = config['in_band_mode']
                            log.warning(f"'{sample_entry_type}' 条目缺少有效 '{config_box_name}' Box，将回退到“带内”模式 ({self.mode})。")
                        else:
                            # 如果不支持带内模式 (例如 AV1)，则为错误
                            raise ValueError(f"'{sample_entry_type}' 条目缺少必需的 '{config_box_name}' Box。")
                else:
                    # 无需配置 box 的格式 -> 带内模式
                    self.mode = config['in_band_mode']
                    log.info(f"检测到 '{sample_entry_type}' 采样条目，将使用“带内”模式 ({self.mode})。")

                found_codec = True
                break # 使用第一个找到的受支持的视频轨道

        if not found_codec:
            raise ValueError(f"在 'stsd' Box 中未找到任何受支持的视频采样条目 (H.264, H.265, AV1)。")

        # --- 2. 构建采样地图 ---
        # (这部分逻辑与编解码器无关，保持不变)
        stsz_payload = tables.get('stsz'); stsz_payload.seek(4)
        sample_size, sample_count = struct.unpack('>II', stsz_payload.read(8))
        sample_sizes = []
        if sample_size == 0:
            if sample_count > 0:
                sample_sizes = struct.unpack(f'>{sample_count}I', stsz_payload.read(sample_count * 4))

        stss_payload = tables.get('stss'); keyframe_set = set()
        if stss_payload:
            stss_payload.seek(4); entry_count = struct.unpack('>I', stss_payload.read(4))[0]
            if entry_count > 0:
                keyframe_set = set(struct.unpack(f'>{entry_count}I', stss_payload.read(entry_count * 4)))
        else:
            keyframe_set = set(range(1, sample_count + 1))

        co_box = tables.get('stco') or tables.get('co64')
        if not co_box: raise ValueError("未找到 'stco' 或 'co64' box。")
        co_payload = co_box; co_payload.seek(4)
        entry_count = struct.unpack('>I', co_payload.read(4))[0]
        unpack_char = '>I' if tables.get('stco') else '>Q'
        chunk_offsets = struct.unpack(f'>{entry_count}{unpack_char[-1]}', co_payload.read(entry_count * struct.calcsize(unpack_char)))

        stsc_payload = tables.get('stsc'); stsc_payload.seek(4)
        entry_count = struct.unpack('>I', stsc_payload.read(4))[0]
        stsc_entries = [struct.unpack('>III', stsc_payload.read(12)) for _ in range(entry_count)]

        stts_payload = tables.get('stts'); stts_payload.seek(4)
        entry_count = struct.unpack('>I', stts_payload.read(4))[0]
        stts_entries = [struct.unpack('>II', stts_payload.read(8)) for _ in range(entry_count)]

        # --- 3. 迭代并创建完整的样本列表 ---
        samples = []
        stsc_idx, sample_idx, current_time = 0, 0, 0
        stts_iter = iter(stts_entries)
        stts_count, stts_duration = next(stts_iter, (0, 0))
        stts_sample_idx_in_entry = 0

        for chunk_idx, chunk_offset in enumerate(chunk_offsets):
            if stsc_idx < len(stsc_entries) - 1 and (chunk_idx + 1) >= stsc_entries[stsc_idx + 1][0]:
                stsc_idx += 1
            _, samples_per_chunk, _ = stsc_entries[stsc_idx]

            current_offset_in_chunk = 0
            for _ in range(samples_per_chunk):
                if sample_idx >= sample_count: break

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

        log.info("完成采样地图构建。共找到 %d 个样本，其中 %d 个是关键帧。", len(self.samples), len(self.keyframes))
