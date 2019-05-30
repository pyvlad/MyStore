"""
This module contains implementations of 'packers' - classes
responsible for converting values into and from the format used
to store them on disk.
"""
import logging
lg = logging.getLogger(__name__)

import os
import gzip
import json
import base64


class BaseValuePacker:
    pass


class CompressedJsonPacker:
    """
    Convert JSON objects to/from compressed binary values.
    """
    @staticmethod
    def pack_value(v):
        if type(v) is not bytes:
            v = gzip.compress(json.dumps(v).encode())
        return v

    @staticmethod
    def unpack_value(v_raw):
        try:
            v = json.loads(gzip.decompress(v_raw).decode())
        except UnicodeDecodeError:
            lg.warning("UnicodeDecodeError while decoding value for key=%s", k)
            v = json.loads(gzip.decompress(v_raw).decode(errors="replace"))
        return v


class BytesBase64Packer:
    """
    Convert binary values to/from base64 encoded text strings.
    """
    @staticmethod
    def pack_value(v):
        return base64.encodebytes(v).decode('ascii')

    @staticmethod
    def unpack_value(v_raw):
        return base64.decodebytes(v_raw.encode('ascii'))
