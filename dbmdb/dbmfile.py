"""
This module contains JsonDbmFile,
a wrapper class around a dbm file which stores values as compressed JSONs.
"""
import logging
lg = logging.getLogger(__name__)

import dbm
import gzip
import json


class JsonDbmFile:
    """
    This class wraps dbm file, which stores Python objects as compressed JSON values.
    Keeps handle and filepath as instance attributes when a dbm file is open.
    """
    def __init__(self, path, mode="c"):
        """
        Create instance and open dbm file at 'path'.
        """
        self._path = path
        self._handle = dbm.open(path, mode)

    def __str__(self):
        return self._path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __setitem__(self, k, v):
        """
        Save a k:v pair in current file.
        The value must be either:
        1. a JSON serializable Python object, or
        2. a compressed serialized JSON.
        """
        if type(v) is not bytes:
            v = gzip.compress(json.dumps(v).encode())
        self._handle[k] = v

    def get(self, k, raw=False):
        """
        Get a value by key from file.
        The key must be either string or bytes.
        Pass raw = True to retrieve raw value (compressed JSON).
        """
        raw_value = self._handle[k]
        if raw:
            return raw_value
        else:
            try:
                value = json.loads(gzip.decompress(raw_value).decode())
            except UnicodeDecodeError:
                lg.warning("UnicodeDecodeError while decoding value for key=%s", k)
                value = json.loads(gzip.decompress(raw_value).decode(errors="replace"))
            return value

    @property
    def path(self):
        return self._path

    def keys(self):
        return self._handle.keys()

    def close(self):
        self._handle.close()
        self._path = self._handle = None
