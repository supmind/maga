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

# --- Unit Tests for service.py methods ---

def test_parse_keyframes_from_stbl(service_instance, moov_data):
    """
    测试 _parse_keyframes_from_stbl 是否能从一个真实的 moov box 数据中正确提取关键帧信息。
    """
    # moov_data 是一个包含整个 moov box 的字节串
    # 我们首先需要用 F4VParser 解析它来获取 moov_box 对象
    moov_box = next(F4VParser.parse(bytes_input=moov_data))

    # 调用被测试的函数
    keyframe_infos = service_instance._parse_keyframes_from_stbl(moov_box)

    # 断言
    assert keyframe_infos is not None, "方法不应返回 None"
    assert isinstance(keyframe_infos, list), "应返回一个列表"
    assert len(keyframe_infos) > 0, "应从 BigBuckBunny 的 moov box 中提取出关键帧"

    # 检查返回列表中的第一个元素是否符合格式
    first_item = keyframe_infos[0]
    assert isinstance(first_item, tuple), "列表中的元素应该是元组"
    assert len(first_item) == 2, "每个元组应包含 keyframe_info 和时间戳字符串"

    keyframe_info, timestamp_str = first_item
    assert isinstance(keyframe_info, KeyframeInfo), "元组的第一个元素应为 KeyframeInfo"
    assert isinstance(timestamp_str, str), "元组的第二个元素应为字符串"

    # 验证 KeyframeInfo 的内容
    assert keyframe_info.pts >= 0, "时间戳（pts）应该是正数"
    assert keyframe_info.pos > 0, "第一个关键帧的偏移量应该大于0"
    assert keyframe_info.size > 0, "帧大小应该大于0"

    # 验证时间戳格式 (HH:MM:SS)
    assert len(timestamp_str) == 8, "时间戳字符串长度应为8"
    assert timestamp_str[2] == ':', "时间戳格式应为 HH:MM:SS"
    assert timestamp_str[5] == ':', "时间戳格式应为 HH:MM:SS"
