# -*- coding: utf-8 -*-
"""
该模块包含 ScreenshotGenerator 类，它使用 PyAV（FFmpeg 的 Python 绑定）
通过发送 H.264 数据包的方式来解码和生成截图。
"""
import logging
import os
from typing import Optional
import av


class ScreenshotGenerator:
    """
    通过向 PyAV 解码器发送 H.264 数据包来处理截图的创建。
    这个类将解码和文件保存的 CPU 和 IO 密集型操作封装起来，以便在异步环境中使用。
    """
    def __init__(self, loop, output_dir='./screenshots_output'):
        """
        初始化生成器。
        :param loop: asyncio 事件循环，用于调度线程池任务。
        :param output_dir: 保存截图的目标目录。
        """
        self.loop = loop
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotGenerator")

    def _save_frame_to_jpeg(self, frame, infohash_hex: str, timestamp_str: str):
        """
        将一个解码后的 PyAV 帧对象保存为 JPG 文件。
        :param frame: PyAV 的 Frame 对象。
        :param infohash_hex: torrent 的 infohash，用于构成唯一的文件名。
        :param timestamp_str: 帧的时间戳字符串，用于构成唯一的文件名。
        """
        # 创建一个唯一的文件名，并将时间戳中的冒号替换为连字符，以确保文件名在所有操作系统上都有效
        output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"
        os.makedirs(self.output_dir, exist_ok=True)
        frame.to_image().save(output_filename)
        self.log.info(f"成功：截图已保存至 {output_filename}")

    def _decode_and_save(self, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        一个同步方法，用于解码单个 H.264 数据包并将其保存为 JPG 文件。
        此方法被设计为在线程池执行器中运行，以避免阻塞 asyncio 事件循环。

        :param extradata: H.264 的 'avcC' box 内容 (SPS/PPS)，如果可用的话。
        :param packet_data: 包含单个视频帧的原始 H.264 数据包。
        :param infohash_hex: torrent 的 infohash。
        :param timestamp_str: 帧的时间戳。
        """
        try:
            # 创建一个 H.264 解码器实例
            codec = av.CodecContext.create('h264', 'r')

            # 如果提供了 'extradata' (来自 'avcC' box)，将其提供给解码器。
            # 这被称为“带外”配置，解码器不需要从视频流中猜测参数，解码效率更高。
            if extradata:
                self.log.debug(f"正在使用 {len(extradata)} 字节的 extradata (带外) 配置解码器...")
                codec.extradata = extradata
            else:
                self.log.debug("未提供 extradata，解码器将尝试从码流 (带内) 中寻找配置。")

            # 将原始数据包装成 PyAV 的 Packet 对象
            packet = av.Packet(packet_data)
            self.log.debug(f"将一个大小为 {packet.size} 字节的数据包送入解码器...")

            # 解码数据包。这可能会返回一个或多个帧。
            frames = codec.decode(packet)

            # 有时解码器需要更多数据才能输出一个完整的帧，第一次调用 decode 可能会返回一个空列表。
            # 在这种情况下，发送一个空的“刷新”包来强制解码器输出其内部缓冲的任何帧。
            if not frames:
                self.log.warning("第一次解码未返回帧，尝试发送一个空的刷新包...")
                try:
                    frames = codec.decode(None) # 发送刷新包
                except av.EOFError:
                    # 在刷新时，如果解码器内部没有更多数据，可能会抛出 EOFError，这是正常现象，可以安全忽略。
                    pass

            # 如果在刷新后仍然没有帧，说明解码失败。
            if not frames:
                self.log.error(f"解码失败：解码器未能从时间戳 {timestamp_str} 的数据包中解码出任何帧。")
                return

            # 我们只取解码出的第一帧来生成截图。
            self._save_frame_to_jpeg(frames[0], infohash_hex, timestamp_str)

        except av.AVError as e:
            # 捕获所有 PyAV 相关的错误，如无效数据、解码器错误等
            self.log.error(f"解码器为 {timestamp_str} 报告错误: {e}")
        except Exception as e:
            # 捕获其他意外错误（如文件系统权限问题），但允许关键错误（如 KeyboardInterrupt）传播
            self.log.exception(f"为帧 {timestamp_str} 进行同步解码/保存时发生意外错误")
            raise e

    async def generate(self, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        业务流程: 异步地生成并保存一个截图。
        它通过在线程池中运行同步的 `_decode_and_save` 方法来实现这一点，
        从而将 CPU 密集型的解码和 IO 密集的文件保存操作与主事件循环解耦。
        """
        self.log.debug(f"正在为时间戳 {timestamp_str} 生成截图")

        # 将 CPU 密集的解码和 IO 密集的文件保存操作委托给执行器线程。
        await self.loop.run_in_executor(
            None, # 使用默认的执行器 (通常是 ThreadPoolExecutor)
            self._decode_and_save,
            extradata,
            packet_data,
            infohash_hex,
            timestamp_str
        )
