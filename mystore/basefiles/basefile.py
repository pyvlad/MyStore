"""
This module contains BaseFile, an abstract base class
which represents basic storage unit.
"""
import logging
lg = logging.getLogger(__name__)

import os
import abc

from mystore.errors import MyStoreError


class BaseFile(metaclass=abc.ABCMeta):
    """
    Abstract Base Class which represents the API of a basic storage unit.
    """
    @property
    @abc.abstractproperty
    def EXTENSION(self):
        pass

    def __init__(self, path, mode, *, wait_time=0.1):
        """
        Create instance and open file at 'path' in specified mode.
        Mode can be "w", "r", "W", "R".
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
    def keys(self):
        """ Load and return list of all keys contained in the file. """

    @abc.abstractmethod
    def items(self):
        """ Load and return list of all k:v pairs (as tuples) contained in the file. """

    @abc.abstractmethod
    def close(self):
        """ Close file and finish all pending operations. """

    def _open(self):
        """ Open file for operations and return handle - accessor to file content. """
        lg.debug("opening new file handle")
        if self.mode == "r":
            return self._open_for_read()
        elif self.mode == "R":
            return self._open_for_read_loop()
        elif self.mode == "w":
            return self._open_for_write()
        elif self.mode == "W":
            return self._open_for_write_loop()
        else:
            raise MyStoreError("Unsupported open mode: %s" % self.mode)

    def _raise_unsupported(self):
        raise MyStoreError("Mode {} is not supported by {}".format(
            self.mode, self.__class__.__name__))

    _open_for_read = _open_for_read_loop = \
    _open_for_write = _open_for_write_loop = \
    _raise_unsupported

    @classmethod
    def all_filepaths(cls, root):
        """
        Generator.
        Get all files of this class in the 'root' directory.
        This is default implementation, which only looks at file extension.
        """
        for dirpath, dirnames, filenames in os.walk(root):
            for fn in filenames:
                filepath = os.path.join(dirpath, fn)
                _, file_extension = os.path.splitext(filepath)
                if file_extension == cls.EXTENSION:
                    yield filepath
