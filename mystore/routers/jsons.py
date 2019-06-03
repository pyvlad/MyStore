"""
This module contains JsonRouter,
router class mapping integers to json files.
"""
import os

from .base import BaseRouter
from .original import OriginalRouter


class JsonRouter(OriginalRouter, BaseRouter):
    EXTENSION = ".json"

    def all_filepaths(self):
        for dirpath, dirnames, filenames in os.walk(self.root_dir):
            for fn in filenames:
                filepath = os.path.join(dirpath, fn)
                _, file_extension = os.path.splitext(filepath)
                if file_extension == self.__class__.EXTENSION:
                    yield filepath
