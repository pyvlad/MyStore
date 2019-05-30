"""
This module contains DbmFile, which represents basic storage unit.
"""
import logging
lg = logging.getLogger(__name__)

import os
import dbm.gnu
import dbm
import json
import time


class BaseFile:
    def __init__(self, path, mode, *, wait_time=0.1):
        """
        Create instance and open file at 'path' in specified mode.
        """
        self.path = path
        self.mode = mode           # "c", "r", "C", "R"
        self.wait_time = wait_time
        self._handle = self._open()

    def __str__(self):
        return self.path

    @property
    def dirname(self):
        return os.path.dirname(self.path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __getitem__(self, k):
        raise NotImplemented

    def __setitem__(self, k, v):
        raise NotImplemented

    def close(self):
        raise NotImplemented

    def _open(self):
        raise NotImplemented

    def keys(self):
        raise NotImplemented

    def items(self):
        raise NotImplemented

    def _create_directory(self):
        try:
            os.makedirs(self.dirname)
            lg.debug("Created directory [%s]", self.dirname)
        except FileExistsError:     # created by another thread/process
            lg.debug("Directory already exists [%s]", self.dirname)



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
    def _open(self):
        lg.debug("opening new file handle")
        if self.mode == "r":
            return dbm.open(self.path, "r")
        elif self.mode == "w":
            try:
                handle = dbm.open(self.path, "c")
            except dbm.gnu.error as e:
                if e.errno == 2:
                    self._create_directory()
                    handle = dbm.open(self.path, "c")
                else:
                    raise
            return handle
        elif self.mode == "R":
            return self._loop_open("r")
        elif self.mode == "W":
            return self._loop_open("c")
        else:
            raise OSError("Unsupported dbm mode: %s" % self.mode)

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



class JsonFile(BaseFile):
    def __getitem__(self, k):
        return self._handle[str(k)]

    def __setitem__(self, k, v):
        self._handle[str(k)] = v

    def close(self):
        if self.mode == "w":
            content = json.dumps(self._handle)
            with open(self.path, "w") as f:
                f.write(content)

    def _open(self):
        lg.debug("opening new file handle")
        if self.mode == "w":
            if not os.path.exists(self.dirname):
                self._create_directory()
        if self.mode in ["w", "r"]:
            if os.path.exists(self.path):
                with open(self.path, "r") as f:
                    content = f.read()
                    content = json.loads(content)
                    return content
            else:
                return {}
        else:
            raise OSError("Mode %s is not supported by JsonFile")

    def keys(self):
        return list(self._handle.keys())

    def items(self):
        return self._handle.items()
