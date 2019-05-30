"""
This module contains Reader, a class responsible for write operations.
"""
import logging
lg = logging.getLogger(__name__)

from .dbmfile import JsonDbmFile
from .misc import DummyThreadlock


class Reader:
    """
    Class responsible for read operations.

    Arguments
    ---------
    threadlock: obj
        pass threadlock to synchronize multiple writers
        and make write operations thread safe
    """
    def __init__(self, threadlock=None):
        self.threadlock = DummyThreadlock() if not threadlock else threadlock
        self.file = None

    def get_value(self, key, filepath, raw=False):
        """
        Arguments
        ---------
        key: int
            Integer key.
        value: bool
            Pass True to retrieve raw compressed JSON.
        """
        str_key = str(key)

        with self.threadlock:
            if self.file is None or self.file.path != filepath:
                lg.debug("closing old dbm file, opening new one")
                if self.file is not None and self.file.path is not None:
                    self.file.close()
                self.file = JsonDbmFile(filepath, mode="r")
            else:
                lg.debug("dbm already open, skipping")
            value = self.file.get(str_key, raw)

        return value

    def close(self):
        """
        Close reader instance:
        (a) close currently opened dbm file
        (b) delete "file" attribute to fail on attempted "get_value" methods
        """
        if self.file is not None and self.file.path is not None:
            self.file.close()
        delattr(self, "file")
        delattr(self, "threadlock")
