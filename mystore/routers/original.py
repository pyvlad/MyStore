"""
This module contains OriginalRouter.
This router class maps integers to base units
according to:
    - base unit size,
    - size of subfolders with base units,
    - and first key (0 or 1).
"""
import logging
lg = logging.getLogger(__name__)

import os
import dbm
import dbm.gnu

from .base import BaseRouter


class OriginalRouter(BaseRouter):
    """
    BaseRouter subclass mapping integer keys to base unit paths.

    Arguments
    ---------
    root_dir: str
        Base directory where tree of dbm files is stored.
    params: dict
        unit_size: int
            Number of key:value items in single dbm file.
        subfolder_size: int
            Maximum number of dbm files in one folder
            (if 0 - no subfolders are created).
        first_key: int
            Integer key to start from (default: 1)

    Example 1:
    >>> router = OriginalRouter(root_dir="/tmp/", \
            params={"unit_size": 10, "subfolder_size":2, "first_key":1}, extension=".dbm")
    >>> router.get_path(22)
    '/tmp/1/0.dbm'

    Example 2:
    >>> router = OriginalRouter(root_dir="/tmp/", \
            params={"unit_size": 10, "subfolder_size":0, "first_key":1}, extension=".dbm")
    >>> router.get_path(22)
    '/tmp/2.dbm'
    """
    def __init__(self, root_dir, params, extension):
        super().__init__(root_dir, params, extension)
        self.unit_size = params["unit_size"]
        self.subfolder_size = params["subfolder_size"]
        self.first_key = params["first_key"]

    def get_path(self, key):
        # 1. derive index of dbm file from key value
        file_index = (key - self.first_key) // self.unit_size

        # 2. derive subfolder name and dbm file name from dbm file index
        #   a. don't use subfolders
        if self.subfolder_size == 0:
            subfolder_name = os.path.curdir
            filename = str(file_index)
        #   b. use subfolders
        elif self.subfolder_size > 0:
            subfolder_index = file_index // self.subfolder_size
            subfolder_name = str(subfolder_index)
            filename = str(file_index - (subfolder_index * self.subfolder_size))

        # 3. derive absolute filename
        filepath = os.path.join(self.root_dir, subfolder_name, "%s" % filename)

        # 4. make it pretty
        filepath = os.path.normpath(filepath)

        return filepath + self.extension


if __name__ == "__main__":
    import doctest
    doctest.testmod()
