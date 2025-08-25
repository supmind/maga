# -*- coding: utf-8 -*-
import os
import pytest
import struct
from screenshot.pymp4parse import F4VParser, BoxHeader

# 定义测试视频文件的路径
TEST_VIDEO_PATH = os.path.join(os.path.dirname(__file__), 'Big_Buck_Bunny_720_10s_10MB.mp4')

def find_box_data(file_path, box_type_str):
    """
    一个辅助函数，用于在 MP4 文件中查找指定类型的 box 并返回其内容。
    这对于隔离 'moov' box 进行单元测试非常有用。
    """
    with open(file_path, 'rb') as f:
        while True:
            header_bytes = f.read(8)
            if not header_bytes:
                break

            size, box_type_bytes = struct.unpack('>I4s', header_bytes)
            box_type = box_type_bytes.decode('ascii', errors='ignore')

            if size == 1: # 64-bit size
                size_bytes = f.read(8)
                if not size_bytes: break
                size = struct.unpack('>Q', size_bytes)[0]
                content_size = size - 16
            else:
                content_size = size - 8

            if box_type == box_type_str:
                # 找到了！返回整个 box 的数据 (header + content)
                f.seek(f.tell() - 8 - (8 if size == 1 else 0)) # 回到 box 的起始位置
                return f.read(size)

            # 跳转到下一个 box
            f.seek(content_size, os.SEEK_CUR)
    return None

@pytest.fixture(scope="module")
def moov_data():
    """
    一个 pytest fixture，它在模块测试开始前运行一次，
    负责找到并加载 'moov' box 的数据，供后续测试用例使用。
    """
    data = find_box_data(TEST_VIDEO_PATH, 'moov')
    assert data is not None, f"测试失败：未能在 {TEST_VIDEO_PATH} 文件中找到 'moov' box。"
    return data

def test_file_exists():
    """确认测试用的视频文件存在。"""
    assert os.path.exists(TEST_VIDEO_PATH), f"测试视频文件不存在于: {TEST_VIDEO_PATH}"

def test_parse_moov_box(moov_data):
    """
    核心单元测试：测试 F4VParser 能否正确解析一个真实的 'moov' box。
    """
    # 使用 F4VParser 解析 'moov' box 的字节数据
    # next() 用于获取生成器产生的第一个（也是唯一一个）box
    moov_box = next(F4VParser.parse(bytes_input=moov_data))

    # --- 断言验证 ---
    assert moov_box is not None
    assert moov_box.header.box_type == 'moov'
    assert len(moov_box.children) > 0, "moov box 应该包含子 box"

    # 验证 'mvhd' (Movie Header) box 是否存在且被正确添加为属性
    assert hasattr(moov_box, 'mvhd'), "解析后的 moov_box 对象应有名为 'mvhd' 的属性"

    # 验证是否至少有一个 'trak' (Track) box
    assert hasattr(moov_box, 'trak'), "moov_box 应至少包含一个 'trak' box"

    # 获取第一个 track box 进行更深入的检查
    trak_box = moov_box.trak[0] if isinstance(moov_box.trak, list) else moov_box.trak
    assert trak_box.header.box_type == 'trak'

    # 验证 track box 的内部结构
    assert hasattr(trak_box, 'tkhd'), "trak_box 应包含 'tkhd' (Track Header)"
    assert hasattr(trak_box, 'mdia'), "trak_box 应包含 'mdia' (Media)"

    mdia_box = trak_box.mdia
    assert hasattr(mdia_box, 'mdhd'), "'mdia' box 应包含 'mdhd' (Media Header)"
    assert hasattr(mdia_box, 'hdlr'), "'mdia' box 应包含 'hdlr' (Handler Reference)"
    assert hasattr(mdia_box, 'minf'), "'mdia' box 应包含 'minf' (Media Information)"

    minf_box = mdia_box.minf
    assert hasattr(minf_box, 'stbl'), "'minf' box 应包含 'stbl' (Sample Table)"

    # 'stbl' (Sample Table) 是最关键的部分，包含了所有样本信息
    stbl_box = minf_box.stbl
    assert stbl_box.header.box_type == 'stbl'

    # 验证 stbl 中所有必要的子 box 都被成功解析
    expected_stbl_children = ['stsd', 'stts', 'stss', 'stsc', 'stsz', 'stco']
    for child_type in expected_stbl_children:
        assert hasattr(stbl_box, child_type), f"'stbl' box 缺少必要的子 box: '{child_type}'"

    # 对几个关键的表进行内容断言
    stss_box = stbl_box.stss # 关键帧表
    assert stss_box.header.box_type == 'stss'
    assert len(stss_box.entries) > 0, "'stss' box (关键帧表) 不应为空"

    stco_box = stbl_box.stco # 块偏移表
    assert stco_box.header.box_type == 'stco'
    assert len(stco_box.entries) > 0, "'stco' box (块偏移表) 不应为空"

def test_find_child_box(moov_data):
    """
    测试辅助函数 find_child_box 是否能正确地在解析树中导航。
    """
    moov_box = next(F4VParser.parse(bytes_input=moov_data))

    # 定义一个到 'stbl' box 的有效路径
    path_to_stbl = ['trak', 'mdia', 'minf', 'stbl']

    stbl_box = F4VParser.find_child_box(moov_box, path_to_stbl)
    assert stbl_box is not None, "使用有效路径应能找到 'stbl' box"
    assert stbl_box.header.box_type == 'stbl'

    # 定义一个无效路径
    path_to_nothing = ['trak', 'foo', 'bar']

    nothing_box = F4VParser.find_child_box(moov_box, path_to_nothing)
    assert nothing_box is None, "使用无效路径应返回 None"
