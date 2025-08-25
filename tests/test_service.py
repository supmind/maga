# -*- coding: utf-8 -*-
import pytest
import io
import struct
import os
from unittest.mock import MagicMock, patch

# 由于 service.py 依赖 pymp4parse，我们需要确保它能被导入
from screenshot.service import ScreenshotService, KeyframeInfo
from screenshot.pymp4parse import F4VParser

# --- Fixtures and Test Data ---

@pytest.fixture(scope="module")
def service_instance():
    """提供一个 ScreenshotService 的实例，用于调用其静态和实例方法。"""
    # 使用 MagicMock 来模拟 loop，因为我们不需要一个真实的事件循环
    return ScreenshotService(loop=MagicMock())

@pytest.fixture(scope="module")
def moov_data():
    """从真实的 MP4 文件中加载 moov box 数据，复用 pymp4parse 测试的逻辑。"""
    test_video_path = os.path.join(os.path.dirname(__file__), 'Big_Buck_Bunny_720_10s_10MB.mp4')

    # 这是一个辅助函数，用于在文件中查找并读取 box
    def find_box_data(file_path, box_type_str):
        with open(file_path, 'rb') as f:
            while True:
                header_bytes = f.read(8)
                if not header_bytes: break
                size, box_type_bytes = struct.unpack('>I4s', header_bytes)
                if size == 1:
                    size = struct.unpack('>Q', f.read(8))[0]
                    content_size = size - 16
                else:
                    content_size = size - 8

                if box_type_bytes.decode('ascii') == box_type_str:
                    f.seek(f.tell() - (16 if size == 1 else 8))
                    return f.read(size)
                f.seek(content_size, os.SEEK_CUR)
        return None

    data = find_box_data(test_video_path, 'moov')
    assert data is not None, "测试需要 'moov' box 数据"
    return data

def create_fake_box(box_type, content):
    """一个简单的辅助函数，用于创建假的 MP4 box 字节串。"""
    header = struct.pack('>I4s', 8 + len(content), box_type.encode('ascii'))
    return header + content

# --- Unit Tests for service.py methods ---

def test_parse_mp4_boxes(service_instance):
    """测试 _parse_mp4_boxes 能否正确地从字节流中解析出 box。"""
    # 创建一个由三个假 box 组成的字节流
    box1_content = b'\x01\x02\x03\x04'
    box2_content = b'\x05\x06'
    box3_content = b'\x07\x08\x09\x0a\x0b\x0c'

    fake_file_content = (
        create_fake_box('box1', box1_content) +
        create_fake_box('box2', box2_content) +
        create_fake_box('box3', box3_content)
    )
    reader = io.BytesIO(fake_file_content)

    # 调用解析器
    boxes = list(service_instance._parse_mp4_boxes(reader))

    # 断言
    assert len(boxes) == 3

    # 验证第一个 box
    assert boxes[0][0] == 'box1'
    assert boxes[0][2] == len(box1_content) # content_size

    # 验证第二个 box
    assert boxes[1][0] == 'box2'
    assert boxes[1][1] == 8 + len(box1_content) + 8 # content_pos

    # 验证第三个 box
    assert boxes[2][0] == 'box3'
    assert boxes[2][2] == len(box3_content)

def test_is_fmp4(service_instance):
    """测试 _is_fmp4 函数能否正确识别 fMP4 和标准 MP4。"""
    # 1. 测试 fMP4 (moov 包含 mvex)
    mvex_box = create_fake_box('mvex', b'')
    moov_content_fmp4 = create_fake_box('trak', b'') + mvex_box
    file_content_fmp4 = create_fake_box('ftyp', b'isom') + create_fake_box('moov', moov_content_fmp4)
    reader_fmp4 = io.BytesIO(file_content_fmp4)
    # 赋予 reader 一个 file_size 属性，以模拟 TorrentFileReader 的行为
    reader_fmp4.file_size = len(file_content_fmp4)

    assert service_instance._is_fmp4(reader_fmp4) is True

    # 2. 测试标准 MP4 (moov 不含 mvex)
    moov_content_std = create_fake_box('trak', b'')
    file_content_std = create_fake_box('ftyp', b'isom') + create_fake_box('moov', moov_content_std)
    reader_std = io.BytesIO(file_content_std)
    reader_std.file_size = len(file_content_std)

    assert service_instance._is_fmp4(reader_std) is False

def test_parse_sidx(service_instance):
    """测试 _parse_sidx 能否正确解析一个手动构建的 sidx box。"""
    # 手动构建一个 sidx box (version 0)
    # 包含 header, version, flags, reference_id, timescale, ept, fo, reserved, ref_count
    # 和两个 reference entry
    sidx_content = (
        b'\x00' +                # version = 0
        b'\x00\x00\x00' +        # flags
        b'\x00\x00\x00\x01' +    # reference_ID = 1
        b'\x00\x00\x23\x28' +    # timescale = 9000
        b'\x00\x00\x00\x00' +    # earliest_presentation_time = 0
        b'\x00\x00\x00\x50' +    # first_offset = 80
        b'\x00\x00' +            # reserved
        b'\x00\x02' +            # reference_count = 2
        # 第一个 reference
        b'\x00\x01\x86\xa0' +    # reference_size = 100000
        b'\x00\x00\x46\x50' +    # subsegment_duration = 18000
        b'\x80\x00\x00\x00' +    # starts_with_SAP = 1 (True)
        # 第二个 reference
        b'\x00\x01\xd4\xc0' +    # reference_size = 120000
        b'\x00\x00\x46\x50' +    # subsegment_duration = 18000
        b'\x00\x00\x00\x00'     # starts_with_SAP = 0 (False)
    )
    sidx_box = create_fake_box('sidx', sidx_content)
    reader = io.BytesIO(sidx_box)

    # 调用解析函数。注意：_parse_sidx 的参数是 content_pos 和 content_size
    box_type, content_pos, content_size = next(service_instance._parse_mp4_boxes(reader))

    reader.seek(0) # 重置 reader
    references, timescale, first_offset = service_instance._parse_sidx(reader, content_pos, content_size)

    assert timescale == 9000
    assert first_offset == 80
    assert len(references) == 2

    assert references[0]['size'] == 100000
    assert references[0]['pts'] == 0
    assert references[0]['is_keyframe'] is True

    assert references[1]['size'] == 120000
    assert references[1]['pts'] == 18000
    assert references[1]['is_keyframe'] is False

def test_extract_keyframes_from_index(service_instance, moov_data):
    """
    测试 _extract_keyframes_from_index 是否能从一个真实的 moov box 中提取出关键帧信息。
    这里需要模拟 TorrentFileReader。
    """
    # 创建一个模拟的 TorrentFileReader
    mock_reader = MagicMock()
    mock_reader.read.return_value = moov_data # 当 read() 被调用时，返回 moov 数据
    mock_reader.seek.return_value = 0

    # 调用被测试的函数
    keyframe_infos = service_instance._extract_keyframes_from_index(mock_reader)

    # 断言
    assert keyframe_infos is not None
    assert isinstance(keyframe_infos, list)
    assert len(keyframe_infos) > 0, "应从 BigBuckBunny 的 moov box 中提取出关键帧"

    # 检查返回列表中的第一个元素是否符合格式
    first_item = keyframe_infos[0]
    assert isinstance(first_item, tuple)
    assert len(first_item) == 2

    keyframe_info, timestamp_str = first_item
    assert isinstance(keyframe_info, KeyframeInfo)
    assert isinstance(timestamp_str, str)

    # 验证 KeyframeInfo 的内容
    assert keyframe_info.pts >= 0
    assert keyframe_info.pos > 0 # 第一个关键帧的偏移量应该大于0
    assert keyframe_info.size > 0

    # 验证时间戳格式 (HH:MM:SS)
    assert len(timestamp_str) == 8
    assert timestamp_str[2] == ':'
    assert timestamp_str[5] == ':'
