# -*- coding: utf-8 -*-
"""
对 screenshot/generator.py 中截图生成器功能的单元测试。
"""
import pytest
import os
import asyncio
from PIL import Image

from worker.screenshot.generator import ScreenshotGenerator
from worker.screenshot.extractor import KeyframeExtractor
from tests.screenshot.test_extractor import moov_atom_data # 复用 extractor 测试的 fixture

# --- 测试资源 ---
TEST_VIDEO_PATH = "tests/assets/test_video.mp4"

@pytest.fixture(scope="module")
def video_packet_data(moov_atom_data):
    """
    一个 Pytest fixture，从测试视频中提取解码第一帧所需的所有信息。
    包括：codec_name, extradata, 和第一个数据包 (packet) 的内容。
    """
    # 1. 使用 KeyframeExtractor 获取元数据
    extractor = KeyframeExtractor(moov_atom_data)
    assert extractor.samples, "测试视频中未找到样本"

    # 2. 读取第一个样本（即视频帧）的原始数据
    first_sample = extractor.samples[0]
    with open(TEST_VIDEO_PATH, "rb") as f:
        f.seek(first_sample.offset)
        packet_data = f.read(first_sample.size)

    return {
        "codec_name": extractor.codec_name,
        "extradata": extractor.extradata,
        "packet_data": packet_data,
        "nal_length_size": extractor.nal_length_size,
        "mode": extractor.mode
    }

# --- 测试用例 ---

@pytest.mark.asyncio
async def test_screenshot_generator_creates_valid_jpeg(video_packet_data):
    """
    测试 ScreenshotGenerator 是否能成功解码一个数据包并生成一个有效的 JPEG 字节串。
    """
    # 1. 准备测试环境和参数
    loop = asyncio.get_running_loop()
    infohash_hex = "test_generator_infohash"
    timestamp_str = "00-00-00"

    # 创建一个 Future 用于回调函数，以便我们可以等待它完成
    callback_called = asyncio.Future()

    async def on_success_callback(infohash, image_bytes, ts):
        """测试用的回调函数，当被调用时，设置 Future 的结果。"""
        if not callback_called.done():
            callback_called.set_result((infohash, image_bytes, ts))

    # 2. 实例化并运行生成器
    # output_dir 已废弃，但为保持构造函数兼容性而传入
    generator = ScreenshotGenerator(loop=loop, output_dir="", on_success=on_success_callback)

    packet_data = video_packet_data["packet_data"]

    # 如果是 avc1 格式, 需要转换为 Annex B
    if video_packet_data["mode"] == 'avc1':
        annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
        nal_length_size = video_packet_data["nal_length_size"]
        while cursor < len(packet_data):
            nal_length = int.from_bytes(packet_data[cursor : cursor + nal_length_size], 'big')
            cursor += nal_length_size
            nal_data = packet_data[cursor : cursor + nal_length]
            annexb_data.extend(start_code + nal_data)
            cursor += nal_length
        packet_data = bytes(annexb_data)

    await generator.generate(
        codec_name=video_packet_data["codec_name"],
        extradata=video_packet_data["extradata"],
        packet_data=packet_data,
        infohash_hex=infohash_hex,
        timestamp_str=timestamp_str
    )

    # 3. 等待并验证回调结果
    try:
        infohash, image_bytes, ts = await asyncio.wait_for(callback_called, timeout=2)
        assert infohash == infohash_hex
        assert ts == timestamp_str
        assert isinstance(image_bytes, bytes)
        assert len(image_bytes) > 100, "生成的 JPEG 字节数过少，可能为空或无效"
    except asyncio.TimeoutError:
        pytest.fail("ScreenshotGenerator 的 on_success 回调在2秒内未被调用。")
