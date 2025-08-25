""" MP4 Parser based on:
http://download.macromedia.com/f4v/video_file_format_spec_v10_1.pdf

@author: Alastair McCormack
@license: MIT License

"""

import bitstring
from collections import namedtuple
import logging
import six

log = logging.getLogger(__name__)
log.setLevel(logging.WARN)

# ==============================================================================
# Box classes
# ==============================================================================

class MixinDictRepr(object):
    def __repr__(self, *args, **kwargs):
        # A simple repr for boxes that just shows their attributes
        return f"{self.__class__.__name__}: {self.__dict__}"

class GenericContainerBox(MixinDictRepr):
    """ A generic box that contains other boxes. """
    def __init__(self, header):
        self.header = header
        self.children = []

    def add_child(self, box):
        self.children.append(box)
        box_type_str = box.header.box_type.decode('utf-8') if isinstance(box.header.box_type, bytes) else box.header.box_type
        if box_type_str.isidentifier():
            if hasattr(self, box_type_str):
                existing = getattr(self, box_type_str)
                if isinstance(existing, list):
                    existing.append(box)
                else:
                    setattr(self, box_type_str, [existing, box])
            else:
                setattr(self, box_type_str, box)

class UnImplementedBox(MixinDictRepr):
    type = "unimplemented"

class FullBox(MixinDictRepr):
    """ A box that has a version and flags. """
    def __init__(self, header):
        self.header = header
        self.version = 0
        self.flags = 0

class SyncSampleBox(FullBox): # stss
    type = "stss"
    def __init__(self, header):
        super().__init__(header)
        self.entries = []

class TimeToSampleBox(FullBox): # stts
    type = "stts"
    def __init__(self, header):
        super().__init__(header)
        self.entries = []

class SampleToChunkBox(FullBox): # stsc
    type = "stsc"
    def __init__(self, header):
        super().__init__(header)
        self.entries = []

class SampleSizeBox(FullBox): # stsz
    type = "stsz"
    def __init__(self, header):
        super().__init__(header)
        self.sample_size = 0
        self.entries = []

class ChunkOffsetBox(FullBox): # stco
    type = "stco"
    def __init__(self, header):
        super().__init__(header)
        self.entries = []

class ChunkLargeOffsetBox(FullBox): # co64
    type = "co64"
    def __init__(self, header):
        super().__init__(header)
        self.entries = []

class MovieBox(GenericContainerBox): type = "moov"
class TrackBox(GenericContainerBox): type = "trak"
class MediaBox(GenericContainerBox): type = "mdia"
class MediaInformationBox(GenericContainerBox): type = "minf"
class SampleTableBox(GenericContainerBox): type = "stbl"

BoxHeader = namedtuple("BoxHeader", ["box_size", "box_type", "header_size", "start_offset"])

# ==============================================================================
# Parser
# ==============================================================================

class F4VParser(object):
    @classmethod
    def parse(cls, filename=None, bytes_input=None, file_input=None, offset_bytes=0):
        box_lookup = {
            "moov": cls._parse_container_box, "trak": cls._parse_container_box,
            "mdia": cls._parse_container_box, "minf": cls._parse_container_box,
            "stbl": cls._parse_container_box,
            "stss": cls._parse_stss, "stts": cls._parse_stts,
            "stsc": cls._parse_stsc, "stsz": cls._parse_stsz,
            "stco": cls._parse_stco, "co64": cls._parse_co64,
        }

        if filename: bs = bitstring.ConstBitStream(filename=filename, offset=offset_bytes * 8)
        elif bytes_input: bs = bitstring.ConstBitStream(bytes=bytes_input, offset=offset_bytes * 8)
        else: bs = bitstring.ConstBitStream(auto=file_input, offset=offset_bytes * 8)

        while bs.pos < bs.len:
            try:
                header = cls._read_box_header(bs)
            except bitstring.ReadError:
                break

            box_type_str = header.box_type
            parse_function = box_lookup.get(box_type_str, cls._parse_unimplemented)

            try:
                yield parse_function(bs, header)
            except (bitstring.ReadError, ValueError) as e:
                log.error(f"Failed to parse box '{box_type_str}' due to: {e}. Skipping.")
                bs.bytepos = header.start_offset + header.header_size + header.box_size
                continue

    @staticmethod
    def _read_box_header(bs):
        header_start_pos = bs.bytepos
        size, box_type_bytes = bs.readlist("uint:32, bytes:4")
        box_type = box_type_bytes.decode('ascii', errors='ignore')
        header_size = 8
        if size == 1:
            size = bs.read("uint:64")
            header_size = 16
        content_size = size - header_size if size > header_size else 0
        return BoxHeader(box_size=content_size, box_type=box_type, header_size=header_size, start_offset=header_start_pos)

    @classmethod
    def _parse_container_box(cls, bs, header):
        box_type_map = {
            "moov": MovieBox, "trak": TrackBox, "mdia": MediaBox,
            "minf": MediaInformationBox, "stbl": SampleTableBox,
        }
        box = box_type_map.get(header.box_type, GenericContainerBox)(header)
        content_bs = bs.read(header.box_size * 8)
        for child in cls.parse(bytes_input=content_bs.bytes):
            box.add_child(child)
        return box

    @staticmethod
    def _parse_full_box_header(bs):
        version = bs.read("uint:8")
        flags = bs.read("uint:24")
        return version, flags

    @classmethod
    def _parse_stss(cls, bs, header):
        stss = SyncSampleBox(header)
        content_bs = bs.read(header.box_size * 8)
        stss.version, stss.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        stss.entries = [content_bs.read("uint:32") for _ in range(entry_count)]
        return stss

    @classmethod
    def _parse_stts(cls, bs, header):
        stts = TimeToSampleBox(header)
        content_bs = bs.read(header.box_size * 8)
        stts.version, stts.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        stts.entries = [(content_bs.read("uint:32"), content_bs.read("uint:32")) for _ in range(entry_count)]
        return stts

    @classmethod
    def _parse_stsc(cls, bs, header):
        stsc = SampleToChunkBox(header)
        content_bs = bs.read(header.box_size * 8)
        stsc.version, stsc.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        stsc.entries = [(content_bs.read("uint:32"), content_bs.read("uint:32"), content_bs.read("uint:32")) for _ in range(entry_count)]
        return stsc

    @classmethod
    def _parse_stsz(cls, bs, header):
        stsz = SampleSizeBox(header)
        content_bs = bs.read(header.box_size * 8)
        stsz.version, stsz.flags = cls._parse_full_box_header(content_bs)
        stsz.sample_size = content_bs.read("uint:32")
        sample_count = content_bs.read("uint:32")
        if stsz.sample_size == 0:
            stsz.entries = [content_bs.read("uint:32") for _ in range(sample_count)]
        return stsz

    @classmethod
    def _parse_stco(cls, bs, header):
        stco = ChunkOffsetBox(header)
        content_bs = bs.read(header.box_size * 8)
        stco.version, stco.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        stco.entries = [content_bs.read("uint:32") for _ in range(entry_count)]
        return stco

    @classmethod
    def _parse_co64(cls, bs, header):
        co64 = ChunkLargeOffsetBox(header)
        content_bs = bs.read(header.box_size * 8)
        co64.version, co64.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        co64.entries = [content_bs.read("uint:64") for _ in range(entry_count)]
        return co64

    @staticmethod
    def _parse_unimplemented(bs, header):
        ui = UnImplementedBox()
        ui.header = header
        bs.pos += header.box_size * 8
        return ui

    @staticmethod
    def find_child_box(box, path):
        current_box = box
        for box_type in path:
            found_child = None
            if hasattr(current_box, box_type):
                child = getattr(current_box, box_type)
                found_child = child[0] if isinstance(child, list) else child
            if found_child:
                current_box = found_child
            else:
                return None
        return current_box
