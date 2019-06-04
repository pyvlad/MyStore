"""
This module contains FolderFile, BaseFile implementation with
folder of plain files as basic storage unit.
"""
import logging
lg = logging.getLogger(__name__)

import os

from .basefile import BaseFile
from mystore.errors import BaseUnitDoesNotExist


class DirFile(BaseFile):
    @property
    def dirname(self):
        return self.path

    def __getitem__(self, k):
        try:
            with open(os.path.join(self.dirname, str(k)), "rb") as f:
                return f.read()
        except FileDoesNotExist:
            return KeyError("No such value.")

    def __setitem__(self, k, v):
        with open(os.path.join(self.dirname, str(k)), "wb") as f:
            f.write(v)

    def keys(self):
        return os.listdir(self.dirname)

    def items(self):
        return {k: self[k] for k in self.keys()}.items()

    def close(self):
        pass

    # ******* implementation details *******
    def _open_for_read(self):
        if not os.path.exists(self.path):
            raise BaseUnitDoesNotExist

    def _open_for_read_loop(self):
        if not os.path.exists(self.path):
            raise BaseUnitDoesNotExistd

    def _open_for_write(self):
        if not os.path.exists(self.path):
            self._create_directory()

    def _open_for_write_loop(self):
        if not os.path.exists(self.path):
            self._create_directory()
