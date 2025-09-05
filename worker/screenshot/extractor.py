# -*- coding: utf-8 -*-
"""
本模块提供了 KeyframeExtractor 类，其核心功能是从 MP4 文件的 'moov' atom 中
提取视频关键帧的元数据。它负责解析复杂的 MP4 内部结构，以定位解码所需的
所有信息，支持 H.264, H.265 (HEVC), 和 AV1 等多种现代视频编解码器。
"""
import struct
import logging
from io import BytesIO
from collections import namedtuple
from typing import BinaryIO, Generator, Tuple, Optional

# --- 日志配置 ---
log = logging.getLogger(__name__)

# --- 数据结构定义 ---
# SampleInfo: 代表一个媒体样本（通常是一帧视频或音频）的详细信息。
SampleInfo = namedtuple("SampleInfo", ["offset", "size", "is_keyframe", "index", "pts"])
# Keyframe: 代表一个视频关键帧（或同步点）的简化信息。
Keyframe = namedtuple("Keyframe", ["index", "sample_index", "pts", "timescale"])


class KeyframeExtractor:
    """
    一个健壮的 MP4 视频关键帧提取器。

    它的核心功能是仅通过解析 'moov' box 的二进制数据，就能提取出视频流中所有
    关键帧的位置、大小、时间戳和解码所需配置等元数据。这使得我们可以在不下载
    整个视频文件的情况下，精确地请求和解码任意一个关键帧。

    主要工作流程：
    1.  解析 'moov' box，找到视频轨道 ('trak')。
    2.  从视频轨道中找到 'stbl' (Sample Table) box，这是所有元数据的核心。
    3.  依次解析 'stbl' 中的多个子表：
        - 'stsd' (Sample Description): 获取解码器类型 (H.264/HEVC/AV1) 和解码器
          配置数据 (extradata，如 SPS/PPS)。
        - 'stsz' (Sample Size): 获取每一帧的大小。
        - 'stss' (Sync Sample): 获取哪些帧是关键帧。
        - 'stco'/'co64' (Chunk Offset): 获取数据块在文件中的位置。
        - 'stsc' (Sample to Chunk): 描述了帧如何被组织成数据块。
        - 'stts' (Time to Sample): 获取每一帧的显示时间戳 (PTS)。
    4.  将以上所有信息整合成一个完整的 `samples` 列表，其中每个元素都代表一帧，
        包含其所有元数据。
    5.  从 `samples` 列表中筛选出关键帧，生成一个简化的 `keyframes` 列表。
    """

    def __init__(self, moov_data: Optional[bytes]):
        """
        初始化提取器。
        如果提供了 `moov_data`，将立即开始解析。
        如果 `moov_data` 为 None，则对象将被初始化为空白状态（用于从序列化状态恢复）。

        :param moov_data: 包含 'moov' box 完整内容的字节串。
        """
        if moov_data:
            self.moov_stream = BytesIO(moov_data)
        else:
            self.moov_stream = None

        self.keyframes: list[Keyframe] = []
        self.samples: list[SampleInfo] = []
        self.extradata: Optional[bytes] = None  # 解码器特定配置数据 (如 SPS/PPS)
        self.codec_name: Optional[str] = None   # 编解码器名称 (如 'h264', 'hevc')
        self.mode: str = "unknown"              # 码流模式 (如 'avc1', 'avc3')
        self.nal_length_size: int = 4           # NAL 单元长度字段的字节数
        self.timescale: int = 1000              # 媒体时间尺度
        self.duration_pts: int = 0              # 视频轨道总时长 (以 timescale 为单位)

        if moov_data:
            try:
                self._parse_structure()
            except Exception as e:
                log.error("解析 'moov' box 时发生严重错误: %s", e, exc_info=True)
                raise # 将异常向上传播，由服务层处理

    def _parse_boxes(self, stream: BinaryIO) -> Generator[Tuple[str, BytesIO], None, None]:
        """
        一个 MP4 box 解析器生成器。
        它能迭代地从给定的二进制流中解析出 box，并处理 32 位和 64 位大小的 box。
        """
        while True:
            current_offset = stream.tell()
            header_data = stream.read(8)
            if not header_data or len(header_data) < 8: break

            size, box_type_bytes = struct.unpack('>I4s', header_data)
            box_type = box_type_bytes.decode('ascii', 'ignore')
            header_size = 8

            if size == 1: # 64位大小
                size_64_data = stream.read(8)
                if not size_64_data: break
                size = struct.unpack('>Q', size_64_data)[0]
                header_size = 16
            elif size == 0: # Box 延伸到文件末尾
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

            yield box_type, BytesIO(payload_content)

    def _find_box_payload(self, stream: BinaryIO, box_path: list[str]) -> Optional[BytesIO]:
        """
        从流的当前位置开始，递归地查找一个按路径指定的嵌套 Box，并返回其 payload。
        例如 `box_path=['mdia', 'hdlr']` 会查找 `stream` -> `mdia` -> `hdlr`。
        """
        if not box_path: return stream
        target_box, remaining_path = box_path[0], box_path[1:]
        for box_type, payload in self._parse_boxes(stream):
            if box_type == target_box:
                return self._find_box_payload(payload, remaining_path)
        return None

    def _parse_structure(self):
        """
        解析 'moov' box 的核心逻辑。
        这是一个多步骤的过程，旨在找到视频轨道并构建出一个完整的“采样地图”，
        该地图包含了每个视频帧的位置、大小、时间和类型等所有必要信息。
        """
        stream = self.moov_stream

        # 'moov' box 本身可能被另一个 'moov' box 包裹，这里处理这种情况
        stream.seek(0)
        _, box_type = struct.unpack('>I4s', stream.read(8))
        stream.seek(0)
        if box_type == b'moov':
            for b_type, payload in self._parse_boxes(stream):
                if b_type == 'moov':
                    stream = payload; break
        moov_payload = stream; moov_payload.seek(0)

        # 步骤 1: 在 'moov' 中找到视频轨道 ('trak')
        # 通过查找 'hdlr' box 并检查其处理器类型是否为 'vide' 来识别视频轨道。
        trak_payload = None
        for t_type, t_payload_iter in self._parse_boxes(moov_payload):
            if t_type == 'trak':
                current_pos = t_payload_iter.tell()
                hdlr_payload = self._find_box_payload(t_payload_iter, ['mdia', 'hdlr'])
                t_payload_iter.seek(current_pos)
                if hdlr_payload:
                    hdlr_payload.seek(8)
                    if hdlr_payload.read(4) == b'vide':
                        trak_payload = t_payload_iter; break
        if not trak_payload: raise ValueError("在 'moov' Box 中未找到有效的视频轨道。")

        # 步骤 2: 从 'mdhd' (Media Header) box 获取 timescale 和总时长
        # Timescale 是定义媒体时间单位的关键参数。
        mdhd_payload = self._find_box_payload(trak_payload, ['mdia', 'mdhd'])
        if mdhd_payload:
            version = struct.unpack('>B', mdhd_payload.read(1))[0]
            if version == 0:
                mdhd_payload.seek(12)
                self.timescale = struct.unpack('>I', mdhd_payload.read(4))[0]
                self.duration_pts = struct.unpack('>I', mdhd_payload.read(4))[0]
            else:  # version 1
                mdhd_payload.seek(20)
                self.timescale = struct.unpack('>I', mdhd_payload.read(4))[0]
                self.duration_pts = struct.unpack('>Q', mdhd_payload.read(8))[0]
        else:
            log.warning("在视频轨道中未找到 'mdhd' box，无法确定 timescale 和 duration。")
        trak_payload.seek(0)

        # 步骤 3: 找到 'stbl' (Sample Table) box，它是所有采样信息的核心容器。
        stbl_payload = self._find_box_payload(trak_payload, ['mdia', 'minf', 'stbl'])
        if not stbl_payload: raise ValueError("在视频轨道中未找到 'stbl' Box。")

        # 步骤 4: 解析 'stbl' 内的所有子表。
        tables = {b_type: b_payload for b_type, b_payload in self._parse_boxes(stbl_payload)}

        # 步骤 5: 建立采样地图并获取解码器配置。
        self._build_sample_map_and_config(tables)

    def _build_sample_map_and_config(self, tables: dict):
        """
        解析 stbl 内的各个表，以获取解码器配置 (extradata) 并构建完整的采样地图。
        这是整个类中最复杂的部分，因为它需要关联来自多个表的信息。
        """
        # --- 1. 获取解码器配置 ---
        stsd_payload = tables.get('stsd') # Sample Description Box
        if not stsd_payload: raise ValueError("在 'stbl' Box 中未找到 'stsd' Box。")
        stsd_payload.seek(8)

        codec_configs = {
            'avc1': {'codec': 'h264', 'config_box': 'avcC'}, 'avc3': {'codec': 'h264'},
            'hvc1': {'codec': 'hevc', 'config_box': 'hvcC'}, 'hev1': {'codec': 'hevc', 'config_box': 'hvcC'},
            'av01': {'codec': 'av1', 'config_box': 'av1C'},
        }
        found_codec = False
        for sample_entry_type, sample_entry_payload in self._parse_boxes(stsd_payload):
            if sample_entry_type in codec_configs:
                config = codec_configs[sample_entry_type]
                self.codec_name = config['codec']
                config_box_name = config.get('config_box')

                if config_box_name: # 带外配置 (Out-of-band)
                    sample_entry_payload.seek(78)
                    config_box_payload = self._find_box_payload(sample_entry_payload, [config_box_name])
                    if config_box_payload and len(config_box_payload.getvalue()) > 5:
                        self.mode = 'avc1'
                        config_data = config_box_payload.getvalue()

                        if self.codec_name == 'h264':
                            self.extradata = config_data
                            self.nal_length_size = (config_data[4] & 0x03) + 1
                        elif self.codec_name == 'hevc':
                            self.extradata = self._parse_hvcC_extradata(config_data)
                            self.nal_length_size = (config_data[21] & 0x03) + 1
                        elif self.codec_name == 'av1':
                            self.extradata = config_data
                    else: # 回退到带内配置
                        self.mode = 'avc3'
                else: # 带内配置 (In-band)
                    self.mode = 'avc3'

                found_codec = True; break
        if not found_codec: raise ValueError("在 'stsd' Box 中未找到任何受支持的视频采样条目 (H.264, H.265, AV1)。")

        # --- 2. 构建采样地图 ---
        # stsz: Sample Size Box - 包含每个样本的大小。
        stsz_payload = tables.get('stsz'); stsz_payload.seek(4)
        sample_size, sample_count = struct.unpack('>II', stsz_payload.read(8))
        sample_sizes = list(struct.unpack(f'>{sample_count}I', stsz_payload.read(sample_count * 4))) if sample_size == 0 else []

        # stss: Sync Sample Box - 包含所有关键帧的索引。如果不存在，则所有帧都是关键帧。
        stss_payload = tables.get('stss'); keyframe_set = set()
        if stss_payload:
            stss_payload.seek(4); entry_count = struct.unpack('>I', stss_payload.read(4))[0]
            if entry_count > 0: keyframe_set = set(struct.unpack(f'>{entry_count}I', stss_payload.read(entry_count * 4)))
        else: keyframe_set = set(range(1, sample_count + 1))

        # stco/co64: Chunk Offset Box - 包含每个数据块 (chunk) 在文件中的偏移量。
        co_box = tables.get('stco') or tables.get('co64')
        if not co_box: raise ValueError("未找到 'stco' 或 'co64' box。")
        co_payload = co_box; co_payload.seek(4)
        entry_count = struct.unpack('>I', co_payload.read(4))[0]
        unpack_char = '>I' if tables.get('stco') else '>Q'
        chunk_offsets = struct.unpack(f'>{entry_count}{unpack_char[-1]}', co_payload.read(entry_count * struct.calcsize(unpack_char)))

        # stsc: Sample to Chunk Box - 定义了样本如何组织成块。
        stsc_payload = tables.get('stsc'); stsc_payload.seek(4)
        entry_count = struct.unpack('>I', stsc_payload.read(4))[0]
        stsc_entries = [struct.unpack('>III', stsc_payload.read(12)) for _ in range(entry_count)]

        # stts: Time to Sample Box - 定义每个样本的持续时间，用于计算时间戳。
        stts_payload = tables.get('stts'); stts_payload.seek(4)
        entry_count = struct.unpack('>I', stts_payload.read(4))[0]
        stts_entries = [struct.unpack('>II', stts_payload.read(8)) for _ in range(entry_count)]

        # --- 3. 迭代并创建完整的样本列表 ---
        # 这是最关键的逻辑，它将上述所有表的信息组合起来，为每个样本计算出精确的偏移量、大小和时间戳。
        samples, stsc_idx, sample_idx, current_time = [], 0, 0, 0
        stts_iter = iter(stts_entries); stts_count, stts_duration = next(stts_iter, (0, 0)); stts_sample_idx_in_entry = 0

        for chunk_idx, chunk_offset in enumerate(chunk_offsets):
            if stsc_idx < len(stsc_entries) - 1 and (chunk_idx + 1) >= stsc_entries[stsc_idx + 1][0]: stsc_idx += 1
            _, samples_per_chunk, _ = stsc_entries[stsc_idx]
            current_offset_in_chunk = 0
            for _ in range(samples_per_chunk):
                if sample_idx >= sample_count: break
                while stts_sample_idx_in_entry >= stts_count:
                    stts_sample_idx_in_entry -= stts_count; current_time += stts_count * stts_duration
                    stts_count, stts_duration = next(stts_iter, (sample_count, 0))
                pts = current_time + stts_sample_idx_in_entry * stts_duration
                stts_sample_idx_in_entry += 1
                size = sample_sizes[sample_idx] if sample_size == 0 else sample_size
                is_keyframe = (sample_idx + 1) in keyframe_set
                samples.append(SampleInfo(chunk_offset + current_offset_in_chunk, size, is_keyframe, sample_idx + 1, pts))
                current_offset_in_chunk += size; sample_idx += 1

        self.samples = samples
        keyframe_samples = [s for s in self.samples if s.is_keyframe]
        self.keyframes = [Keyframe(i, s.index, s.pts, self.timescale) for i, s in enumerate(keyframe_samples)]
        log.info("完成采样地图构建。共找到 %d 个样本，其中 %d 个是关键帧。", len(self.samples), len(self.keyframes))

    def _parse_hvcC_extradata(self, config_data: bytes) -> bytes:
        """
        解析 hvcC box，提取 VPS, SPS, PPS 等参数集，并将其格式化为
        解码器期望的 Annex B 格式的 extradata。

        Annex B 格式要求每个 NAL 单元前都有一个起始码 (0x00000001)。
        """
        stream = BytesIO(config_data)
        stream.seek(22) # 跳转到 numOfArrays 字段
        num_of_arrays = struct.unpack('>B', stream.read(1))[0]

        annexb_extradata = bytearray()
        start_code = b'\x00\x00\x00\x01'

        for _ in range(num_of_arrays):
            # 读取数组信息：array_completeness (1 bit), reserved (1 bit), NAL_unit_type (6 bits)
            stream.read(1)

            num_nalus = struct.unpack('>H', stream.read(2))[0]
            for _ in range(num_nalus):
                nalu_len = struct.unpack('>H', stream.read(2))[0]
                nalu_data = stream.read(nalu_len)
                annexb_extradata.extend(start_code)
                annexb_extradata.extend(nalu_data)

        return bytes(annexb_extradata)
