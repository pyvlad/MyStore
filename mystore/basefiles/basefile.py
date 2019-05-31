"""
This module contains BaseFile, an abstract base class
which represents basic storage unit.
"""
import logging
lg = logging.getLogger(__name__)

import os
import abc


class BaseFile(metaclass=abc.ABCMeta):
    """
    Abstract Base Class which represents the API of a basic storage unit.
    """
    def __init__(self, path, mode, *, wait_time=0.1):
        """
        Create instance and open file at 'path' in specified mode.
        Mode can be "w", "r". "W", "R" are optional.
        """
        self.path = path
        self.mode = mode
        self.wait_time = wait_time
        self._handle = self._open()

    def __str__(self):
        return self.path

    @property
    def dirname(self):
        return os.path.dirname(self.path)

    def _create_directory(self):
        try:
            os.makedirs(self.dirname)
            lg.debug("Created directory [%s]", self.dirname)
        except FileExistsError:     # created by another thread/process
            lg.debug("Directory already exists [%s]", self.dirname)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abc.abstractmethod
    def __getitem__(self, k):
        """ Return value stored in file for specified key. """

    @abc.abstractmethod
    def __setitem__(self, k, v):
        """ Set value to be stored in file for specified key. """

    @abc.abstractmethod
    def _open(self):
        """ Open file for operations and return handle - accessor to file content. """

    @abc.abstractmethod
    def close(self):
        """ Close file and finish all pending operations. """

    @abc.abstractmethod
    def keys(self):
        """ Load and return list of all keys contained in the file. """

    @abc.abstractmethod
    def items(self):
        """ Load and return list of all k:v pairs (as tuples) contained in the file. """
