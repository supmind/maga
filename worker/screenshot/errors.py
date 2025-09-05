# -*- coding: utf-8 -*-
"""
本模块为截图服务定义了一套自定义的、结构化的异常类。

使用自定义异常的好处：
- 提供了比通用 Exception 更明确的错误类型。
- 允许调用者通过 `except` 语句捕获特定的错误场景并进行相应处理。
- 可以在异常中附加额外的上下文信息（如 infohash），便于调试。
"""

class ScreenshotError(Exception):
    """本应用中所有自定义异常的统一基类。"""
    pass


class MP4ParsingError(ScreenshotError):
    """
    在解析 MP4 文件结构（如 box 格式）时发生的底层错误。
    这个错误是上下文无关的，不包含 infohash，因为它在更通用的、
    可能在没有任务上下文的情况下使用的解析函数中被引发。
    """
    pass


class TaskError(ScreenshotError):
    """
    与单个截图任务相关的错误的基类。
    所有与特定 torrent 任务失败相关的异常都应继承此类。
    它强制要求包含 `infohash`，以便在日志和错误处理中提供关键的上下文信息。
    """
    def __init__(self, message: str, infohash: str):
        super().__init__(message)
        self.infohash = infohash

    def __str__(self):
        # 重写 __str__ 方法，自动在错误消息前加上 infohash 前缀。
        return f"[{self.infohash}] {super().__str__()}"


class MetadataTimeoutError(TaskError):
    """在通过 magnet URI 获取 torrent 元数据（.torrent 文件本身）时发生超时，则引发此异常。"""
    pass


class NoVideoFileError(TaskError):
    """当 torrent 元数据成功下载，但在文件列表中找不到合适的视频文件（如 .mp4, .mkv）时引发。"""
    pass


class MoovNotFoundError(TaskError):
    """
    当在视频文件中无法定位到 'moov' atom 时引发。
    'moov' atom 包含解码和播放视频所需的关键元数据。如果它不存在，或位于非标准位置而无法找到，
    则无法继续处理。
    """
    pass


class MoovFetchError(TaskError):
    """在尝试从 torrent 下载 'moov' atom 数据块时，发生 torrent 客户端内部错误时引发。"""
    pass


class MoovParsingError(TaskError):
    """
    在成功下载 'moov' atom 的数据后，在解析其内部结构（如 'trak', 'stbl' 等）时发生错误时引发。
    这通常表示 'moov' atom 数据已损坏或不符合 MP4 规范。
    """
    pass


class FrameDownloadTimeoutError(TaskError):
    """
    在下载生成截图所需的特定视频数据块（pieces）时发生超时，则引发此异常。
    这通常是由于网络问题或缺少 peer 造成的。
    它包含 `resume_data`，理论上允许任务从断点处恢复。
    """
    def __init__(self, message: str, infohash: str, resume_data=None):
        super().__init__(message, infohash)
        self.resume_data = resume_data


class FrameDecodeError(TaskError):
    """当视频帧数据成功下载，但在使用底层解码库（PyAV）进行解码时失败，则引发此异常。
    这可能意味着视频帧数据本身已损坏。"""
    pass


class TorrentClientError(ScreenshotError):
    """
    用于包装来自底层 `libtorrent` 库的、未被分类到其他特定异常中的内部错误。
    这是一个通用的 torrent 客户端相关错误。
    """
    pass
