"""
This module contains FolderFile, BaseFile implementation with
folder of plain files as basic storage unit.
"""
import logging
lg = logging.getLogger(__name__)

import os

from .basefile import BaseFile


class DirFile(BaseFile):
    @property
    def dirname(self):
        return self.path

    def __getitem__(self, k):
        with open(os.path.join(self.dirname, str(k)), "rb") as f:
            return f.read()

    def __setitem__(self, k, v):
        with open(os.path.join(self.dirname, str(k)), "wb") as f:
            f.write(v)

    def close(self):
        pass

    def _open(self):
        lg.debug("opening new file handle")
        if self.mode in ["w", "W"]:
            if not os.path.exists(self.dirname):
                self._create_directory()

    def keys(self):
        return os.listdir(self.dirname)

    def items(self):
        return {k: self[k] for k in self.keys()}.items()
