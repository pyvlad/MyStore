"""
This module contains router classes, i.e. classes
responsible for mapping keys to subpaths where the
values are stored:
- BaseRouter, base class to override.
- OriginalRouter, original router class mapping integers to dbms.
"""
import logging
lg = logging.getLogger(__name__)

import os


class BaseRouter:
    """
    Base class responsible for mapping keys to dbm files in the root directory.

    Arguments
    ---------
    root_directory: str
        Base directory where tree of dbm files is stored.
    params: dict
        Dictoinary with other parameters used by the 'get_path' method.
    """
    def __init__(self, root_directory, params):
        self.root_directory = root_directory
        self.params = params

    def get_path(self, key):
        """
        Returns path to a child DB.
        """
        raise NotImplemented


class OriginalRouter(BaseRouter):
    """
    BaseRouter subclass mapping integer keys to dbm file paths.

    Arguments
    ---------
    root_directory: str
        Base directory where tree of dbm files is stored.
    params: dict
        dbm_size: int
            Number of key-value items in 1 dbm file.
        subfolder_size: int
            Maximum number of dbm files in 1 folder
            (if 0 - no subfolders are created).
        first_key: int
            Integer key to start from (default: 1)
    """
    def __init__(self, root_directory, params):
        super().__init__(root_directory, params)
        self.dbm_size = params["dbm_size"]
        self.subfolder_size = params["subfolder_size"]
        self.first_key = params["first_key"]

    def get_path(self, key):
        """
        Returns dbm path derived from integer key.

        Example 1:
        >>> router = Router(root_directory="/tmp/", dbm_size=10, subfolder_size=2)
        >>> router.get_path(22)
        '/tmp/1/0.dbm'

        Example 2:
        >>> router = Router(root_directory="/tmp/", dbm_size=10, subfolder_size=0)
        >>> router.get_path(22)
        '/tmp/2.dbm'
        """
        # 1. derive index of dbm file from key value
        file_index = (key - self.first_key) // self.dbm_size

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
        filepath = os.path.join(
            os.path.abspath(self.root_directory),
            subfolder_name,
            "%s.dbm" % filename)

        # 4. make it pretty
        filepath = os.path.normpath(filepath)

        return filepath
