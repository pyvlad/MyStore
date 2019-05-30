"""
This module contains Router, a class responsible for mapping integer keys to dbm files.
"""
import logging
lg = logging.getLogger(__name__)

import os
import json


class Router:
    """
    Class responsible for mapping integer keys to dbm files in the root directory.

    Arguments
    ---------
    root_directory: str
        Base directory where to store tree of dbm files.
    dbm_size: int
        Number of key-value items in 1 dbm file.
    subfolder_size: int
        Maximum number of dbm files in 1 folder;
        if 0 - no subfolders are created.
    first_key: int
        Integer key to start from (default: 1)
    """
    def __init__(self, root_directory, dbm_size, subfolder_size, first_key=1):
        self.root_directory = root_directory
        self.dbm_size = dbm_size
        self.subfolder_size = subfolder_size
        self.first_key = first_key

    def get_path_to_dbm(self, key):
        """
        Returns dbm path derived from integer key.

        Example 1:
        >>> router = Router(root_directory="/tmp/", dbm_size=10, subfolder_size=2)
        >>> router.get_path_to_dbm(22)
        '/tmp/1/0.dbm'

        Example 2:
        >>> router = Router(root_directory="/tmp/", dbm_size=10, subfolder_size=0)
        >>> router.get_path_to_dbm(22)
        '/tmp/2.dbm'
        """
        # 1. derive index of dbm file from key value
        file_index = (key - self.first_key) // self.dbm_size

        # 2. derive subfolder name and dbm file name from dbm file index
        #   a. don't use subfolders
        if self.subfolder_size == 0:
            subfolder_name = os.path.curdir
            file_name = str(file_index)
        #   b. use subfolders
        elif self.subfolder_size > 0:
            subfolder_index = file_index // self.subfolder_size
            subfolder_name = str(subfolder_index)
            file_name = str(file_index - (subfolder_index * self.subfolder_size))

        # 3. derive absolute filename
        path_to_dbm = os.path.join(
            os.path.abspath(self.root_directory),
            subfolder_name,
            "%s.dbm" % file_name)

        # 4. make it pretty
        path_to_dbm = os.path.normpath(path_to_dbm)

        return path_to_dbm
