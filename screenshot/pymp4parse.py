# -*- coding: utf-8 -*-
"""
一个轻量级的 MP4/F4V 文件格式解析器。
主要基于 Adob​​e 的视频文件格式规范 V10.1 版本。
http://download.macromedia.com/f4v/video_file_format_spec_v10_1.pdf

该解析器的核心功能是解析 MP4 文件中的 box（或称 atom），特别是 'moov' box 及其子 box，
因为它们包含了定位关键帧所需的元数据。

@author: Alastair McCormack
@license: MIT License
"""

import bitstring
from collections import namedtuple
import logging
import six

# 设置日志记录器
log = logging.getLogger(__name__)
log.setLevel(logging.WARN)

# ==============================================================================
# Box 类定义
# ==============================================================================

class MixinDictRepr(object):
    """
    一个简单的 mixin 类，用于提供一个更具可读性的 __repr__ 方法，
    它会以字典形式显示 box 实例的属性。
    """
    def __repr__(self, *args, **kwargs):
        return f"{self.__class__.__name__}: {self.__dict__}"

class GenericContainerBox(MixinDictRepr):
    """
    通用容器 Box 类。这类 Box 本身不包含具体数据，而是作为其他 Box 的父容器。
    例如 'moov', 'trak' 等。
    """
    def __init__(self, header):
        self.header = header
        self.children = []

    def add_child(self, box):
        """
        向容器中添加一个子 Box。
        为了方便访问，如果子 Box 的类型字符串是合法的 Python 标识符，
        会尝试将其作为一个属性直接添加到实例上。
        例如，一个 'moov' Box 添加了一个 'trak' 子 Box 后，可以通过 `moov_box.trak` 来访问。
        如果存在多个同类型的子 Box，则会将它们存储在一个列表中。
        """
        self.children.append(box)
        # 将 box_type 从字节解码为字符串以便处理
        box_type_str = box.header.box_type.decode('utf-8') if isinstance(box.header.box_type, bytes) else box.header.box_type
        if box_type_str.isidentifier():
            if hasattr(self, box_type_str):
                # 如果已存在同名属性
                existing = getattr(self, box_type_str)
                if isinstance(existing, list):
                    existing.append(box)  # 如果是列表，直接追加
                else:
                    setattr(self, box_type_str, [existing, box])  # 否则，创建一个新列表
            else:
                setattr(self, box_type_str, box)

class UnImplementedBox(MixinDictRepr):
    """
    用于表示解析器尚未实现或不需要解析其具体内容的 Box。
    解析器在遇到未知类型的 Box 时会使用此类，并跳过其内容。
    """
    def __init__(self, header):
        self.header = header
    type = "unimplemented"

class FullBox(MixinDictRepr):
    """
    代表一个 "Full Box"。根据 MP4 规范，Full Box 是标准 Box 的扩展，
    在其内容开始处包含一个8位的版本号（version）和24位的标志（flags）。
    """
    def __init__(self, header):
        self.header = header
        self.version = 0
        self.flags = 0

# --- 以下是各种具体的 Box 类型的定义 ---

class TrackHeaderBox(FullBox): # tkhd
    type = "tkhd"
    def __init__(self, header):
        super().__init__(header)
        self.duration = 0

class MediaHeaderBox(FullBox): # mdhd
    type = "mdhd"
    def __init__(self, header):
        super().__init__(header)
        self.timescale = 1000
        self.duration = 0

class SyncSampleBox(FullBox):  # stss - 同步样本（关键帧）表
    type = "stss"
    def __init__(self, header):
        super().__init__(header)
        self.entries = []  # 存储关键帧的样本编号

class TimeToSampleBox(FullBox):  # stts - 时间-样本映射表
    type = "stts"
    def __init__(self, header):
        super().__init__(header)
        self.entries = []  # 存储每个样本的持续时间信息

class SampleToChunkBox(FullBox):  # stsc - 样本-块映射表
    type = "stsc"
    def __init__(self, header):
        super().__init__(header)
        self.entries = []  # 存储样本到块（Chunk）的映射关系

class SampleSizeBox(FullBox):  # stsz - 样本大小表
    type = "stsz"
    def __init__(self, header):
        super().__init__(header)
        self.sample_size = 0  # 如果所有样本大小相同，则为该值；否则为0
        self.entries = []  # 如果样本大小不同，则存储每个样本的大小

class ChunkOffsetBox(FullBox):  # stco - 32位块偏移表
    type = "stco"
    def __init__(self, header):
        super().__init__(header)
        self.entries = []  # 存储每个块（Chunk）在文件中的偏移位置（32位）

class ChunkLargeOffsetBox(FullBox):  # co64 - 64位块偏移表
    type = "co64"
    def __init__(self, header):
        super().__init__(header)
        self.entries = []  # 存储每个块（Chunk）在文件中的偏移位置（64位），用于大文件

# --- 容器 Box 的具体实现 ---
class MovieBox(GenericContainerBox): type = "moov"
class TrackBox(GenericContainerBox): type = "trak"
class MediaBox(GenericContainerBox): type = "mdia"
class MediaInformationBox(GenericContainerBox): type = "minf"
class SampleTableBox(GenericContainerBox): type = "stbl"

# 使用 namedtuple 定义一个清晰的 BoxHeader 结构
BoxHeader = namedtuple("BoxHeader", [
    "box_size",       # Box 内容的大小（不含头部）
    "box_type",       # Box 的类型 (4个字符)
    "header_size",    # Box 头部的大小 (通常是8或16字节)
    "start_offset"    # Box 在文件流中的起始偏移
])

# ==============================================================================
# 解析器主类
# ==============================================================================

class F4VParser(object):
    @classmethod
    def parse(cls, filename=None, bytes_input=None, file_input=None, offset_bytes=0):
        """
        MP4 Box 解析器的核心方法。这是一个生成器。
        它可以从多种输入源（文件名、字节串、文件对象）中解析出顶层的 Box。
        :param filename: 要解析的文件路径。
        :param bytes_input: 包含 MP4 数据的字节串。
        :param file_input: 一个文件类对象。
        :param offset_bytes: 开始解析的字节偏移量。
        :return: 迭代返回解析出的 Box 对象。
        """
        # box_lookup 将 box 类型字符串映射到相应的解析方法
        box_lookup = {
            # 容器类 Box
            "moov": cls._parse_container_box, "trak": cls._parse_container_box,
            "mdia": cls._parse_container_box, "minf": cls._parse_container_box,
            "stbl": cls._parse_container_box,
            # 数据类 Box
            "tkhd": cls._parse_tkhd, "mdhd": cls._parse_mdhd,
            "stss": cls._parse_stss, "stts": cls._parse_stts,
            "stsc": cls._parse_stsc, "stsz": cls._parse_stsz,
            "stco": cls._parse_stco, "co64": cls._parse_co64,
        }

        # 根据输入类型创建 bitstring.ConstBitStream 对象
        if filename: bs = bitstring.ConstBitStream(filename=filename, offset=offset_bytes * 8)
        elif bytes_input: bs = bitstring.ConstBitStream(bytes=bytes_input, offset=offset_bytes * 8)
        else: bs = bitstring.ConstBitStream(auto=file_input, offset=offset_bytes * 8)

        # 循环读取，直到文件末尾
        while bs.pos < bs.len:
            try:
                header = cls._read_box_header(bs)
            except bitstring.ReadError:
                # 如果读取头部失败（通常是到达文件末尾的填充数据），则中断循环
                break

            box_type_str = header.box_type
            # 查找对应的解析函数，如果未找到，则使用 _parse_unimplemented
            parse_function = box_lookup.get(box_type_str, cls._parse_unimplemented)

            try:
                # 调用解析函数并 yield 结果
                yield parse_function(bs, header)
            except (bitstring.ReadError, ValueError) as e:
                # 如果解析过程中出错，记录错误并跳过这个 Box
                log.warning(f"解析 Box '{box_type_str}' 失败: {e}。正在跳过。")
                # 将流的位置移动到当前 Box 的末尾，继续解析下一个 Box
                bs.bytepos = header.start_offset + header.header_size + header.box_size
                continue

    @staticmethod
    def _read_box_header(bs):
        """
        从比特流中读取一个 Box 的头部信息。
        """
        header_start_pos = bs.bytepos
        # 读取32位的 size 和4字节的 type
        size, box_type_bytes = bs.readlist("uint:32, bytes:4")
        box_type = box_type_bytes.decode('ascii', errors='ignore')
        header_size = 8  # 标准头部大小

        if size == 1:
            # 如果 size 等于1，表示这是一个64位大小的 Box
            size = bs.read("uint:64")
            header_size = 16

        # 计算内容的实际大小
        content_size = size - header_size if size > header_size else 0
        return BoxHeader(box_size=content_size, box_type=box_type, header_size=header_size, start_offset=header_start_pos)

    @classmethod
    def _parse_container_box(cls, bs, header):
        """
        解析一个容器 Box。它会递归地调用 F4VParser.parse 来解析其包含的子 Box。
        """
        box_type_map = {
            "moov": MovieBox, "trak": TrackBox, "mdia": MediaBox,
            "minf": MediaInformationBox, "stbl": SampleTableBox,
        }
        # 创建对应的容器 Box 实例
        box = box_type_map.get(header.box_type, GenericContainerBox)(header)

        # 读取整个 Box 的内容部分到一个新的比特流中
        content_bs = bs.read(header.box_size * 8)
        # 递归解析子 Box
        for child in cls.parse(bytes_input=content_bs.bytes):
            box.add_child(child)
        return box

    @staticmethod
    def _parse_full_box_header(bs):
        """
        解析 Full Box 的 version 和 flags。
        """
        version = bs.read("uint:8")
        flags = bs.read("uint:24")
        return version, flags

    @classmethod
    def _parse_tkhd(cls, bs, header):
        """ 解析 'tkhd' (Track Header Box) """
        box = TrackHeaderBox(header)
        content_bs = bs.read(header.box_size * 8)
        box.version, box.flags = cls._parse_full_box_header(content_bs)

        if box.version == 1:
            content_bs.pos += 8 * (8 + 8 + 4 + 4) # created, modified, track_id, reserved
            box.duration = content_bs.read("uint:64")
        else: # version 0
            content_bs.pos += 4 * (4 + 4 + 4 + 4) # created, modified, track_id, reserved
            box.duration = content_bs.read("uint:32")
        return box

    @classmethod
    def _parse_mdhd(cls, bs, header):
        """ 解析 'mdhd' (Media Header Box) """
        box = MediaHeaderBox(header)
        content_bs = bs.read(header.box_size * 8)
        box.version, box.flags = cls._parse_full_box_header(content_bs)

        if box.version == 1:
            content_bs.pos += 8 * (8 + 8) # created, modified
            box.timescale = content_bs.read("uint:32")
            box.duration = content_bs.read("uint:64")
        else: # version 0
            content_bs.pos += 4 * (4 + 4) # created, modified
            box.timescale = content_bs.read("uint:32")
            box.duration = content_bs.read("uint:32")
        return box

    @classmethod
    def _parse_stss(cls, bs, header):
        """ 解析 'stss' (Sync Sample Box) """
        stss = SyncSampleBox(header)
        content_bs = bs.read(header.box_size * 8)
        stss.version, stss.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        # 读取所有关键帧的样本号
        stss.entries = [content_bs.read("uint:32") for _ in range(entry_count)]
        return stss

    @classmethod
    def _parse_stts(cls, bs, header):
        """ 解析 'stts' (Time to Sample Box) """
        stts = TimeToSampleBox(header)
        content_bs = bs.read(header.box_size * 8)
        stts.version, stts.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        # 读取 (样本数量, 样本时长) 对
        stts.entries = [(content_bs.read("uint:32"), content_bs.read("uint:32")) for _ in range(entry_count)]
        return stts

    @classmethod
    def _parse_stsc(cls, bs, header):
        """ 解析 'stsc' (Sample to Chunk Box) """
        stsc = SampleToChunkBox(header)
        content_bs = bs.read(header.box_size * 8)
        stsc.version, stsc.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        # 读取 (起始块号, 每块样本数, 样本描述索引) 元组
        stsc.entries = [(content_bs.read("uint:32"), content_bs.read("uint:32"), content_bs.read("uint:32")) for _ in range(entry_count)]
        return stsc

    @classmethod
    def _parse_stsz(cls, bs, header):
        """ 解析 'stsz' (Sample Size Box) """
        stsz = SampleSizeBox(header)
        content_bs = bs.read(header.box_size * 8)
        stsz.version, stsz.flags = cls._parse_full_box_header(content_bs)
        stsz.sample_size = content_bs.read("uint:32")
        sample_count = content_bs.read("uint:32")
        # 如果 sample_size 为 0，表示每个样本的大小可能不同，需要从表中读取
        if stsz.sample_size == 0:
            stsz.entries = [content_bs.read("uint:32") for _ in range(sample_count)]
        return stsz

    @classmethod
    def _parse_stco(cls, bs, header):
        """ 解析 'stco' (Chunk Offset Box) - 32位偏移 """
        stco = ChunkOffsetBox(header)
        content_bs = bs.read(header.box_size * 8)
        stco.version, stco.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        stco.entries = [content_bs.read("uint:32") for _ in range(entry_count)]
        return stco

    @classmethod
    def _parse_co64(cls, bs, header):
        """ 解析 'co64' (Chunk Large Offset Box) - 64位偏移 """
        co64 = ChunkLargeOffsetBox(header)
        content_bs = bs.read(header.box_size * 8)
        co64.version, co64.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        co64.entries = [content_bs.read("uint:64") for _ in range(entry_count)]
        return co64

    @staticmethod
    def _parse_unimplemented(bs, header):
        """
        处理未实现的 Box 类型，简单地跳过其内容。
        """
        ui = UnImplementedBox(header)
        # 将流的指针移动到该 Box 的末尾
        bs.pos += header.box_size * 8
        return ui

    @staticmethod
    def find_child_box(box, path):
        """
        一个辅助函数，用于在 Box 树中按路径查找指定的子 Box。
        :param box: 起始的容器 Box。
        :param path: 一个包含 box_type 字符串的列表，表示查找路径。
                     例如: ['trak', 'mdia', 'minf', 'stbl']
        :return: 找到的子 Box，如果未找到则返回 None。
        """
        current_box = box
        for box_type in path:
            found_child = None
            if hasattr(current_box, box_type):
                child = getattr(current_box, box_type)
                # 如果有多个同名子 Box，默认选择第一个
                found_child = child[0] if isinstance(child, list) else child

            if found_child:
                current_box = found_child
            else:
                return None  # 路径中断，未找到
        return current_box
