"""
This module contains available handler functions which
can convert values to all kinds of formats used to store them on disk.
"""
import logging
lg = logging.getLogger(__name__)

import os
import gzip
import json
import base64


def object_to_compressed_json_binary(v):
    """ Convert JSONifiable object to compressed binary value. """
    return gzip.compress(json.dumps(v).encode("utf-8"))


def object_from_compressed_json_binary(v):
    """ Load JSON object from compressed binary value. """
    try:
        v = json.loads(gzip.decompress(v).decode())
    except UnicodeDecodeError:
        lg.warning("UnicodeDecodeError while decoding value")
        v = json.loads(gzip.decompress(v).decode(errors="replace"))
    return v


def bytes_to_base64_string(v):
    """ Convert binary value to base64 encoded text string. """
    return base64.encodebytes(v).decode('ascii')


def bytes_from_base64_string(v):
    """ Convert base64 encoded text string to bytes. """
    return base64.decodebytes(v.encode('ascii'))
