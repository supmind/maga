# -*- coding: utf-8 -*-
"""
对 screenshot/extractor.py 中关键帧提取器功能的单元测试。
"""
import pytest
import struct
from io import BytesIO

from worker.screenshot.extractor import KeyframeExtractor
from worker.screenshot.errors import MP4ParsingError

# --- 测试资源 ---
TEST_VIDEO_PATH = "tests/assets/test_video.mp4"

@pytest.fixture(scope="module")
def moov_atom_data():
    """
    一个 Pytest fixture，从测试视频文件中读取 'moov' atom 的数据。
    这个 fixture 的作用域是 'module'，因此文件只会被读取一次。
    """
    try:
        with open(TEST_VIDEO_PATH, "rb") as f:
            data = f.read()

        stream = BytesIO(data)
        while True:
            header_data = stream.read(8)
            if not header_data:
                break
            size, box_type_bytes = struct.unpack('>I4s', header_data)
            box_type = box_type_bytes.decode('ascii')

            if box_type == 'moov':
                # 回到 box 的起始位置并读取整个 box 的数据
                stream.seek(stream.tell() - 8)
                return stream.read(size)

            # 跳转到下一个 box
            if size == 1: # 64-bit size
                size = struct.unpack('>Q', stream.read(8))[0]
                stream.seek(size - 16, 1)
            else:
                stream.seek(size - 8, 1)

    except FileNotFoundError:
        pytest.fail(f"测试视频文件未找到: {TEST_VIDEO_PATH}。请先运行 'tests/utils/create_test_video.py' 生成该文件。")

    pytest.fail("在测试视频文件中未找到 'moov' atom。")

# --- 测试用例 ---

def test_keyframe_extractor_init(moov_atom_data):
    """
    测试 KeyframeExtractor 在使用有效的 'moov' 数据初始化时，
    是否能正确解析出所有关键元数据。
    """
    assert moov_atom_data is not None, "moov_atom_data fixture未能提供数据"

    extractor = KeyframeExtractor(moov_atom_data)

    # 1. 验证编解码器和配置信息
    assert extractor.codec_name == "h264"
    assert extractor.mode == "avc1" # 'avc1' 表示带外配置
    assert isinstance(extractor.extradata, bytes)
    assert len(extractor.extradata) > 0 # extradata (SPS/PPS) 不应为空
    assert extractor.nal_length_size == 4

    # 2. 验证时间尺度 (在 create_test_video.py 中 FPS=1, timescale 默认为 90000)
    # PyAV 会为 stream 设置一个默认的 timescale，通常是 90k
    assert extractor.timescale > 0

    # 3. 验证样本和关键帧信息
    # 测试视频只有一个帧，它必须是关键帧
    assert len(extractor.samples) == 1, "应只找到一个样本"
    assert len(extractor.keyframes) == 1, "应只找到一个关键帧"

    sample = extractor.samples[0]
    keyframe = extractor.keyframes[0]

    assert sample.is_keyframe is True, "唯一的样本应为关键帧"
    assert sample.index == 1, "样本索引应为 1"
    assert keyframe.sample_index == sample.index, "关键帧应指向正确的样本索引"

def test_extractor_with_invalid_data():
    """
    测试当提供无效或损坏的 'moov' 数据时，KeyframeExtractor 是否会引发异常。
    """
    # 提供一个明显不是 'moov' box 的随机字节串
    invalid_data = b'this is not a valid moov box'
    with pytest.raises(ValueError, match="在 'moov' Box 中未找到有效的视频轨道"):
        KeyframeExtractor(invalid_data)
