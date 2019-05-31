"""
This module contains DbmFile, BaseFile implementation with
json as basic storage unit.
"""
import logging
lg = logging.getLogger(__name__)

import os
import json

from .basefile import BaseFile


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
