"""
This module contains JsonFileUnit, BaseUnit implementation with
JSON as basic storage unit.
"""
import logging
lg = logging.getLogger(__name__)

import os
import json

from .base import BaseUnit
from mystore.errors import BaseUnitDoesNotExist


class JsonFileUnit(BaseUnit):
    EXTENSION = ".json"

    def __getitem__(self, k):
        return self._handle[str(k)]

    def __setitem__(self, k, v):
        self._handle[str(k)] = v

    def keys(self):
        return list(self._handle.keys())

    def items(self):
        return self._handle.items()

    def close(self):
        if self.mode == "w":
            content = json.dumps(self._handle)
            with open(self.path, "w") as f:
                f.write(content)

    # ******* implementation details *******
    def _open_for_read(self):
        if os.path.exists(self.path):
            with open(self.path, "r") as f:
                content = f.read()
            handle = json.loads(content)
        else:
            raise BaseUnitDoesNotExist
        return handle

    def _open_for_write(self):
        if os.path.exists(self.dirname):
            with open(self.path, "r") as f:
                content = f.read()
            handle = json.loads(content)
        else:
            self._create_directory()
            handle = {}
        return handle
