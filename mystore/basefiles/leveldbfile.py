"""
This module contains LeveldbFile, BaseFile implementation with
LevelDB as basic storage unit.

Installation:
# Python package: pip install plyvel
# DB itself: apt-get install libleveldb1v5 libleveldb-dev

Docs:
https://plyvel.readthedocs.io/en/latest/
https://github.com/google/leveldb

Notes about LevelDB:
- Keys and values are arbitrary byte arrays.
- Data is stored sorted by key.
- Multiple changes can be made in one atomic batch (TODO: incorporate this)
- Only a single process (possibly multi-threaded) can access a particular database at a time.
"""
import logging
lg = logging.getLogger(__name__)

import os

import plyvel

from .basefile import BaseFile
from mystore.errors import BaseUnitDoesNotExist


class LeveldbFile(BaseFile):
    EXTENSION = ".lvl"

    def __getitem__(self, k):
        v = self._handle.get(str(k).encode('ascii')) # returns bytes or None
        if v is None:
            raise KeyError
        return v

    def __setitem__(self, k, v):
        self._handle.put(str(k).encode("ascii"), v)

    def close(self):
        self._handle.close()

    def keys(self):
        # a simple for loop, which will return all key/value pairs in lexicographical key order
        return [k.decode('ascii') for k in self._handle.iterator(include_value=False)]

    def items(self):
        with self._handle.iterator() as it:
            for k, v in it:
                yield (k.decode('ascii'), v)

    def _open_for_read(self):
        try:
            handle = plyvel.DB(self.path, create_if_missing=False)
        except plyvel._plyvel.Error as e:
            raise BaseUnitDoesNotExist
        return handle

    def _open_for_write(self):
        # TODO: when another process uses the DB, plyvel._plyvel.IOError is raised
        # message: b'IO error: lock ./LOCK: Resource temporarily unavailable'
        try:
            handle = plyvel.DB(self.path, create_if_missing=True)
        except plyvel._plyvel.Error as e:
            self._create_directory()
            handle = plyvel.DB(self.path, create_if_missing=True)
        return handle

    @classmethod
    def all_filepaths(cls, root):
        for dirpath, dirnames, filenames in os.walk(root):
            _, dir_extension = os.path.splitext(dirpath)
            if dir_extension == cls.EXTENSION:
                yield dirpath
