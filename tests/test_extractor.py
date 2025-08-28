# -*- coding: utf-8 -*-
"""
H264KeyframeExtractor 的单元测试。
"""
import pytest
import struct
from pathlib import Path
from unittest.mock import patch
from screenshot.extractor import H264KeyframeExtractor

# 获取当前测试文件的目录
TEST_DIR = Path(__file__).parent

# --- 测试辅助函数 ---

def build_box(box_type: bytes, payload: bytes) -> bytes:
    """一个简单的辅助函数，用于构建一个 MP4 box。"""
    size = 8 + len(payload)
    return struct.pack('>I4s', size, box_type) + payload

def build_minimal_valid_stbl(tables: dict) -> bytes:
    """构建一个包含必要子表的最小化 'stbl' box。"""
    stbl_payload = b''
    # 确保以正确的顺序构建，尽管解析器不依赖于顺序
    for name in [b'stsd', b'stts', b'stsc', b'stsz', b'stss', b'stco', b'co64']:
        if name in tables:
            stbl_payload += build_box(name, tables[name])
    return build_box(b'stbl', stbl_payload)

# --- 测试 Fixtures ---

@pytest.fixture(scope="module")
def moov_data():
    """从文件加载真实的 moov.dat fixture。"""
    moov_path = TEST_DIR / "fixtures" / "moov.dat"
    with open(moov_path, "rb") as f:
        return f.read()

@pytest.fixture
def minimal_moov_builder():
    """
    提供一个函数，用于以编程方式构建一个最小化的、功能性的 moov box。
    这使得测试特定边缘情况变得容易，而无需依赖外部的二进制 fixture 文件。
    """
    def _builder(stbl_tables: dict):
        # 构建一个最小的 stsd (avc1 + avcC)
        if b'stsd' not in stbl_tables:
            avcc_payload = build_box(b'avcC', b'\x01' + b'\x00' * 10) # 最小的 avcC
            avc1_payload = b'\x00'*78 + avcc_payload
            stsd_payload = b'\x00\x00\x00\x00\x00\x00\x00\x01' + build_box(b'avc1', avc1_payload)
            stbl_tables[b'stsd'] = stsd_payload

        stbl_box = build_minimal_valid_stbl(stbl_tables)

        # 构建一个最小的 hdlr box，指明这是视频轨道 ('vide')
        hdlr_payload = b'\x00\x00\x00\x00\x00\x00\x00\x00' + b'vide' + b'\x00'*12
        hdlr_box = build_box(b'hdlr', hdlr_payload)

        # 构建其他必要的父 box
        minf_box = build_box(b'minf', stbl_box)
        mdhd_box = build_box(b'mdhd', b'\x00'*20 + struct.pack('>I', 90000)) # mdhd box with timescale
        mdia_box = build_box(b'mdia', mdhd_box + hdlr_box + minf_box)
        trak_box = build_box(b'trak', mdia_box)
        moov_box = build_box(b'moov', trak_box)
        return moov_box

    return _builder

# --- 测试类 ---

class TestH264KeyframeExtractor:
    def test_parsing_sintel_moov_atom(self, moov_data):
        """
        测试提取器是否能正确初始化并解析一个真实的 moov box (来自 Sintel 预告片)。
        这作为一个黄金路径（happy path）测试。
        """
        # GIVEN: 一个真实的 moov 数据
        # WHEN: 使用该数据初始化提取器
        try:
            extractor = H264KeyframeExtractor(moov_data)
        except Exception as e:
            pytest.fail(f"H264KeyframeExtractor 初始化失败，出现异常: {e}")

        # THEN: 解析应成功并填充列表
        assert extractor is not None
        # 这些值是通过对 Sintel moov.dat 文件进行已知分析得出的
        assert len(extractor.samples) == 2090, "样本列表的长度应为 2090。"
        assert len(extractor.keyframes) == 11, "关键帧列表的长度应为 11。"

        # AND: 解析出的属性对于标准的 avc1 视频应是正确的
        assert extractor.mode == 'avc1', f"期望模式为 'avc1'，但得到 '{extractor.mode}'"
        assert extractor.nal_length_size == 4, f"期望 NALU 长度为 4 字节，但得到 {extractor.nal_length_size}"
        assert extractor.timescale == 90000, f"期望 timescale 为 90000，但得到 {extractor.timescale}"

        # AND: 关键帧的具体值应是正确的
        first_keyframe = extractor.keyframes[0]
        assert first_keyframe.sample_index == 1, "第一个关键帧应是第一个样本。"
        assert first_keyframe.pts == 0, "第一个关键帧的 PTS 应为 0。"

        last_keyframe = extractor.keyframes[-1]
        assert last_keyframe.sample_index == 1870, "最后一个关键帧的样本索引应为 1870。"
        assert last_keyframe.pts == 5607000, "最后一个关键帧的 PTS 应为 5607000。"

    def test_parsing_no_stss_atom(self, minimal_moov_builder):
        """
        测试当 'stss' (同步样本) box 缺失时，提取器是否正确地将所有样本都视为关键帧。
        """
        # GIVEN: 一个以编程方式构建的、故意缺少 'stss' box 的 moov box。
        # 它包含 3 个样本，每个大小为 100。
        stbl_tables = {
            b'stsz': b'\x00\x00\x00\x00' + struct.pack('>II', 0, 3) + struct.pack('>III', 100, 100, 100),
            b'stco': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>I', 1000),
            b'stsc': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>III', 1, 3, 1),
            b'stts': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>II', 3, 1000),
            # 注意：stss box 被故意省略了
        }
        moov_box_data = minimal_moov_builder(stbl_tables)

        # WHEN: 使用此 moov box 初始化提取器
        extractor = H264KeyframeExtractor(moov_box_data)

        # THEN: 样本数应为 3，并且关键帧数也应为 3
        assert len(extractor.samples) == 3, "应有 3 个样本"
        assert len(extractor.keyframes) == 3, "在没有 stss box 的情况下，所有样本都应是关键帧"
        assert extractor.keyframes[0].sample_index == 1
        assert extractor.keyframes[1].sample_index == 2
        assert extractor.keyframes[2].sample_index == 3

    def test_parsing_with_co64_atom(self, minimal_moov_builder):
        """
        测试提取器是否能正确处理 'co64' box（用于64位块偏移量），
        这对于大于 4GB 的文件是必需的。
        """
        # GIVEN: 一个包含 'co64' box（而不是 'stco'）的 moov box
        large_offset = 0x1_0000_0000 # 一个大于 32 位整数最大值的偏移量
        stbl_tables = {
            b'stsz': b'\x00\x00\x00\x00' + struct.pack('>II', 0, 1) + struct.pack('>I', 100),
            b'co64': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>Q', large_offset), # 64位偏移
            b'stsc': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>III', 1, 1, 1),
            b'stts': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>II', 1, 1000),
            b'stss': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>I', 1),
        }
        moov_box_data = minimal_moov_builder(stbl_tables)

        # WHEN: 使用此 moov box 初始化提取器
        extractor = H264KeyframeExtractor(moov_box_data)

        # THEN: 样本的偏移量应正确反映大的 64 位值
        assert len(extractor.samples) == 1, "应有 1 个样本"
        assert extractor.samples[0].offset == large_offset, "样本偏移量应匹配 co64 中的 64 位值"

    def test_parsing_failure_on_bad_data(self):
        """
        测试当提供损坏或无效的数据时，提取器能优雅地失败而不会崩溃。
        """
        # GIVEN: 损坏的 moov 数据
        bad_moov_data = b'this is not a moov box'

        # WHEN: 使用损坏的数据初始化提取器
        extractor = H264KeyframeExtractor(bad_moov_data)

        # THEN: 它不应该崩溃，并且其内部列表应该为空
        assert extractor is not None
        assert len(extractor.samples) == 0
        assert len(extractor.keyframes) == 0
        assert extractor.extradata is None


class TestH264KeyframeExtractorEdgeCases:
    """针对 H264KeyframeExtractor 的边缘情况和无效输入的测试。"""

    def test_init_fails_gracefully_with_no_stbl(self, minimal_moov_builder):
        """测试：当 'stbl' box 缺失时，提取器应失败但不会崩溃。"""
        # GIVEN: 一个没有 'stbl' box 的 moov box
        # 通过传递一个空字典来构建一个不含 stbl 的 minf
        minf_box = build_box(b'minf', b'')
        mdia_box = build_box(b'mdia', build_box(b'hdlr', b'\x00'*8 + b'vide' + b'\x00'*12) + minf_box)
        trak_box = build_box(b'trak', mdia_box)
        moov_box = build_box(b'moov', trak_box)

        # WHEN: 使用此数据初始化
        # THEN: 它应该记录一个错误并返回一个空实例
        with patch('screenshot.extractor.log') as mock_log:
            extractor = H264KeyframeExtractor(moov_box)
            assert len(extractor.samples) == 0
            assert len(extractor.keyframes) == 0
            mock_log.error.assert_called_once()
            assert "在视频轨道中未找到 'stbl' Box" in mock_log.error.call_args[0][0]

    def test_init_fails_gracefully_with_no_video_track(self):
        """测试：当 moov box 中没有视频轨道 ('trak' with 'vide' handler) 时。"""
        # GIVEN: 一个只有音轨的 moov box
        hdlr_payload = b'\x00\x00\x00\x00\x00\x00\x00\x00' + b'soun' + b'\x00'*12 # 'soun' handler
        hdlr_box = build_box(b'hdlr', hdlr_payload)
        mdia_box = build_box(b'mdia', hdlr_box)
        trak_box = build_box(b'trak', mdia_box)
        moov_box = build_box(b'moov', trak_box)

        # WHEN: 使用此数据初始化
        # THEN: 它应该记录一个错误并返回一个空实例
        with patch('screenshot.extractor.log') as mock_log:
            extractor = H264KeyframeExtractor(moov_box)
            assert len(extractor.samples) == 0
            assert len(extractor.keyframes) == 0
            mock_log.error.assert_called_once()
            assert "在 'moov' Box 中未找到有效的视频轨道" in mock_log.error.call_args[0][0]

    def test_init_fails_gracefully_with_missing_critical_stbl_child(self, minimal_moov_builder):
        """测试：当 'stbl' 缺少一个关键子 box (如 stsz) 时。"""
        # GIVEN: 一个 'stbl' 缺少 'stsz' 的 moov box
        stbl_tables = {
            # stsz is missing
            b'stco': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>I', 1000),
            b'stsc': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>III', 1, 3, 1),
            b'stts': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>II', 3, 1000),
            b'stss': b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>I', 1),
        }
        moov_box_data = minimal_moov_builder(stbl_tables)

        # WHEN & THEN: 它应该能处理这个错误并返回一个空实例
        with patch('screenshot.extractor.log') as mock_log:
            extractor = H264KeyframeExtractor(moov_box_data)
            assert len(extractor.samples) == 0
            assert len(extractor.keyframes) == 0
            # 错误可能在解析时被捕获，所以检查日志
            assert mock_log.error.called
