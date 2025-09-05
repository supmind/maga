# -*- coding: utf-8 -*-
"""
本测试模块旨在对视频解码的核心逻辑进行单元测试，以验证对多种视频
编解码器的支持。

与集成测试不同，本模块直接测试 `KeyframeExtractor` 和 `ScreenshotGenerator`，
完全绕过了 `TorrentClient` 和网络层，使得测试更快速、稳定，且专注于解码逻辑。
"""
import os
import io
import struct
import asyncio
import tempfile
import pytest
from typing import BinaryIO, Generator, Tuple

from worker.screenshot.extractor import KeyframeExtractor
from worker.screenshot.generator import ScreenshotGenerator
from config import Settings

# --- 测试配置 ---
TEST_ASSETS_DIR = "tests/assets"
TEST_VIDEOS = [
    "test_h264.mp4",
    "test_hevc.mp4",
    pytest.param("test_av1.mp4", marks=pytest.mark.xfail(reason="AV1 decoding fails in PyAV under test conditions")),
]

# --- 辅助函数 ---
def _parse_mp4_boxes(stream: BinaryIO) -> Generator[Tuple[str, bytes], None, None]:
    """
    一个简化的 MP4 box 解析器，用于从视频文件中提取 box。
    这是从 ScreenshotService 中复制并简化的版本，专用于测试。
    """
    while True:
        header_data = stream.read(8)
        if not header_data or len(header_data) < 8:
            break
        size, box_type_bytes = struct.unpack('>I4s', header_data)
        box_type = box_type_bytes.decode('ascii', 'ignore')

        if size == 1:
            size = struct.unpack('>Q', stream.read(8))[0]
            header_size = 16
        else:
            header_size = 8

        payload_size = size - header_size
        payload = stream.read(payload_size)
        yield box_type, payload

def get_moov_atom(video_data: bytes) -> bytes:
    """从给定的视频文件数据中查找并返回 'moov' atom 的内容。"""
    stream = io.BytesIO(video_data)
    for box_type, payload in _parse_mp4_boxes(stream):
        if box_type == 'moov':
            return payload
    raise ValueError("在视频文件中未找到 'moov' atom。")

# --- Pytest Fixtures ---
@pytest.fixture
def settings():
    """提供一个临时的、用于测试的配置实例。"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Settings(output_dir=temp_dir)

# --- 测试用例 ---
@pytest.mark.parametrize("video_filename", TEST_VIDEOS)
@pytest.mark.asyncio
async def test_codec_decoding_logic(settings, video_filename: str):
    """
    一个参数化的单元测试，验证对不同编码格式的视频的核心解析和解码能力。

    :param settings: 由 fixture 提供的 Settings 实例。
    :param video_filename: 由 pytest.mark.parametrize 提供的视频文件名。
    """
    video_path = os.path.join(TEST_ASSETS_DIR, video_filename)
    assert os.path.exists(video_path), f"测试视频文件不存在: {video_path}"

    with open(video_path, "rb") as f:
        video_data = f.read()

    # 1. 提取 'moov' atom 并测试 KeyframeExtractor
    moov_data = get_moov_atom(video_data)
    extractor = KeyframeExtractor(moov_data)

    assert extractor.codec_name is not None, "未能从 moov atom 中解析出编解码器名称"
    assert len(extractor.keyframes) > 0, "未能从 moov atom 中解析出任何关键帧"
    print(f"[{video_filename}] KeyframeExtractor 成功: Codec={extractor.codec_name}, Keyframes={len(extractor.keyframes)}")

    # 2. 准备解码所需的数据
    keyframe = extractor.keyframes[0]
    sample_info = extractor.samples[keyframe.sample_index - 1]
    packet_data_bytes = video_data[sample_info.offset : sample_info.offset + sample_info.size]

    if extractor.mode == 'avc1':
        annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
        while cursor < len(packet_data_bytes):
            nal_length = int.from_bytes(packet_data_bytes[cursor : cursor + extractor.nal_length_size], 'big')
            cursor += extractor.nal_length_size
            nal_data = packet_data_bytes[cursor : cursor + nal_length]
            annexb_data.extend(start_code + nal_data)
            cursor += nal_length
        packet_data = bytes(annexb_data)
    else:
        packet_data = packet_data_bytes

    # 3. 设置回调和事件来捕获截图生成的结果
    generated_files = []
    file_generated_event = asyncio.Event()

    async def on_success_callback(infohash, image_bytes, timestamp_str):
        print(f"测试回调被触发，收到 {len(image_bytes)} 字节的图片数据。")
        generated_files.append(image_bytes)
        file_generated_event.set()

    # 4. 创建 ScreenshotGenerator 并执行解码
    generator = ScreenshotGenerator(
        loop=asyncio.get_running_loop(),
        output_dir=settings.output_dir,
        on_success=on_success_callback
    )

    # 在 generate 调用上加上 try/except 块，以便在解码失败时能提供更清晰的测试失败信息
    try:
        await generator.generate(
            codec_name=extractor.codec_name,
            extradata=extractor.extradata,
            packet_data=packet_data,
            infohash_hex="test-" + video_filename,
            timestamp_str="00-00-00"
        )
    except Exception as e:
        pytest.fail(f"为 {video_filename} 解码时发生意外错误: {e}", pytrace=True)

    # 5. 等待回调被触发并验证结果
    try:
        await asyncio.wait_for(file_generated_event.wait(), timeout=5)
    except asyncio.TimeoutError:
        pytest.fail(f"为 {video_filename} 生成截图后，成功回调未在5秒内被触发。")

    assert len(generated_files) == 1, "成功回调被触发，但未记录任何文件"
    image_data = generated_files[0]
    assert isinstance(image_data, bytes), "回调未传递 bytes 类型的数据"
    assert len(image_data) > 100, f"截图数据大小异常，仅为 {len(image_data)} 字节" # 检查非空且有一定大小
    print(f"[{video_filename}] 验证成功: 已在内存中生成截图。")
