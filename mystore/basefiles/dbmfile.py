"""
This module contains DbmFile, BaseFile implementation with
gdbm as basic storage unit.
"""
import logging
lg = logging.getLogger(__name__)

import dbm.gnu
import dbm
import time

from .basefile import BaseFile
from mystore.errors import BaseUnitDoesNotExist


class DbmFile(BaseFile):
    def __getitem__(self, k):
        return self._handle[str(k)]

    def __setitem__(self, k, v):
        self._handle[str(k)] = v

    def close(self):
        lg.debug("closing old file handle")
        self._handle.close()

    def keys(self):
        return [k.decode() for k in self._handle.keys()]

    def items(self):
        return {k: self[k] for k in self.keys()}.items()

    # ******* implementation details *******
    def _open_for_read(self):
        try:
            handle = dbm.open(self.path, "r")
        except dbm.error as e:
            if str(e).startswith("need 'c' or 'n'"):
                raise BaseUnitDoesNotExist
            raise
        return handle

    def _open_for_read_loop(self):
        try:
            handle = self._loop_open("r")
        except dbm.error as e:
            if str(e).startswith("need 'c' or 'n'"):
                raise BaseUnitDoesNotExist
            raise
        return handle

    def _open_for_write(self):
        try:
            handle = dbm.open(self.path, "c")
        except dbm.gnu.error as e:
            if e.errno == 2:
                self._create_directory()
                handle = dbm.open(self.path, "c")
            else:
                raise
        return handle

    def _open_for_write_loop(self):
        return self._loop_open("c")

    def _loop_open(self, mode):
        """
        Implementation of "R" and "W" modes.
        This modes wait if a dbm file is locked rather than fail with an Exception.
        """
        loop_count = 0
        while True:
            try:
                handle = dbm.open(self.path, mode)
            except dbm.gnu.error as e:
                if e.errno == 11:
                    lg.debug("File locked by writer: %s", self.path)
                    time.sleep(self.wait_time)
                elif e.errno == 2:
                    lg.debug("Directory does not exist: %s", self.dirname)
                    if mode == "c":
                        self._create_directory()
                        continue
                    else:
                        raise # don't create new directories in read mode
                else:
                    raise
            except dbm.error as e:
                if str(e) == "db type could not be determined" and loop_count == 0:
                    # file either corrupted or is being created by another process
                    lg.debug("File type could not be determined: %s", self.path)
                    time.sleep(self.wait_time)
                    loop_count += 1     # fail on second try (probably corrupt)
                else:
                    raise
            else:
                break
        return handle
