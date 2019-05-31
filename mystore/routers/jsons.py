"""
This module contains OriginalRouter,
router class mapping integers to dbm files.
"""
import os

from .original import OriginalRouter


class JsonRouter(OriginalRouter):
    EXTENSION = ".json"

    def all_filepaths(self):
        for dirpath, dirnames, filenames in os.walk(self.root_dir):
            for fn in filenames:
                filepath = os.path.join(dirpath, fn)
                _, file_extension = os.path.splitext(filepath)
                if file_extension == self.__class__.EXTENSION:
                    yield filepath
