# -*- coding: utf-8 -*-
"""
本模块负责实际的截图生成过程。

核心组件是 `ScreenshotGenerator` 类，它利用 PyAV 库来解码视频流。
由于 PyAV 的解码操作是 CPU 密集型的同步阻塞操作，因此我们将这些操作
放在一个单独的线程池中执行，以避免阻塞 asyncio 事件循环。
"""
import logging
import os
from typing import Optional
import av  # PyAV 库，用于音视频处理

# --- 配置日志 ---
log = logging.getLogger(__name__)


class ScreenshotGenerator:
    """
    通过向 PyAV 解码器提供数据包来处理截图的创建。
    此类现在支持 H.264, H.265/HEVC, 和 AV1。

    此类封装了与 PyAV 的所有交互。它接收解码所需的数据（编解码器名称、extradata 和数据包），
    并在一个独立的线程中执行解码和文件保存，以防止阻塞主事件循环。
    """
    def __init__(self, loop, output_dir='./screenshots_output'):
        """
        初始化生成器。
        :param loop: asyncio 事件循环，用于调度线程池任务。
        :param output_dir: 保存生成截图的目录。
        """
        self.loop = loop
        self.output_dir = output_dir

    def _save_frame_to_jpeg(self, frame: av.VideoFrame, infohash_hex: str, timestamp_str: str):
        """
        将一个已解码的 PyAV 帧同步保存为 JPG 文件。
        这是一个阻塞 I/O 操作，因此应在工作线程中运行。
        """
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"

        try:
            # PyAV 的 to_image() 方法依赖于 Pillow 库来创建图像对象
            frame.to_image().save(output_filename, "JPEG")
            log.info("成功：截图已保存至 %s", output_filename)
        except Exception:
            log.exception("保存帧到文件 %s 时发生错误。", output_filename)
            raise

    def _decode_and_save(self, codec_name: str, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        同步解码单个视频数据包并将其保存为 JPG 图像。
        此方法被设计为在线程池执行器 (`run_in_executor`) 中运行，以避免阻塞主线程。

        :param codec_name: 要使用的编解码器名称 (例如 'h264', 'hevc', 'av1')。
        :param extradata: 从容器元数据中提取的解码器配置信息。如果为 None，则为带内模式。
        :param packet_data: 包含一个或多个帧的原始数据包。
        :param infohash_hex: 用于命名文件的 infohash。
        :param timestamp_str: 用于命名文件的时间戳。
        """
        if not codec_name:
            log.error("解码失败：未提供编解码器名称。")
            return
        try:
            # 1. 根据名称动态创建解码器上下文
            log.debug("正在为编解码器 '%s' 创建解码器上下文...", codec_name)
            codec = av.CodecContext.create(codec_name, 'r')  # 'r' 表示读取（解码）模式

            # 2. 配置解码器
            if extradata:
                # '带外' (Out-of-band) 模式：解码器配置在码流之外提供。
                log.debug("正在使用 %d 字节的 extradata (带外) 配置解码器...", len(extradata))
                codec.extradata = extradata
            else:
                # '带内' (In-band) 模式：配置信息内嵌在码流中。
                log.debug("未提供 extradata，解码器将尝试从码流 (带内) 中寻找配置。")

            # 3. 将数据包装成 PyAV 的 Packet 对象
            packet = av.Packet(packet_data)
            log.debug("将一个大小为 %d 字节的数据包送入解码器...", packet.size)

            # 4. 解码数据包
            frames = codec.decode(packet)

            # 5. 处理解码器延迟（获取缓存的帧）
            if not frames:
                log.warning("第一次解码未返回帧，尝试发送一个空的刷新包...")
                try:
                    frames = codec.decode(None)  # 发送刷新包
                except av.EOFError:
                    pass

            if not frames:
                log.error("解码失败：解码器未能从时间戳 %s 的数据包中解码出任何帧。", timestamp_str)
                return

            self._save_frame_to_jpeg(frames[0], infohash_hex, timestamp_str)

        except av.error.InvalidDataError as e:
            log.error("解码器报告无效数据 (时间戳: %s, 编解码器: %s): %s", timestamp_str, codec_name, e)
        except Exception:
            log.exception("为帧 %s (编解码器: %s) 进行同步解码/保存时发生未知错误", timestamp_str, codec_name)
            raise

    async def generate(self, codec_name: str, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        异步地启动截图生成过程。

        此方法是外部调用的主要入口点。它通过 `run_in_executor` 将
        CPU 密集型和阻塞的 `_decode_and_save` 方法调度到线程池中执行，
        从而使主 asyncio 事件循环保持非阻塞状态。
        """
        log.debug("正在为时间戳 %s (编解码器: %s) 安排截图生成任务", timestamp_str, codec_name)

        await self.loop.run_in_executor(
            None,
            self._decode_and_save,
            codec_name,
            extradata,
            packet_data,
            infohash_hex,
            timestamp_str
        )
