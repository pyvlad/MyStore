"""
This module contains the DB class.
This is the main class of the package, it represents a database.
"""
import logging
lg = logging.getLogger(__name__)

import os
import sys
import json

from .basefile import DbmFile, JsonFile
from .routers import OriginalRouter, JsonRouter
from .packers import CompressedJsonPacker, BytesBase64Packer
from .cursors import Reader, Writer


DBMDB_FILENAME = ".dbmdb.json"
CONFIG_FILENAME = "config"
_REGISTRY = {
    cls.__name__: cls
    for cls in [
        OriginalRouter,
        DbmFile,
        CompressedJsonPacker,
        JsonFile,
        JsonRouter,
        BytesBase64Packer
]}


class MyStoreError(OSError):
    pass


def register_cls(cls):
    global _REGISTRY
    _REGISTRY[cls.__name__] = cls


class DB:
    """
    Main class which represents the key:value store.
    """
    def __init__(self, router, basefile_cls=DbmFile, packer_cls=CompressedJsonPacker):
        self.router = router
        self.root = self.router.root_directory
        self.basefile_cls = basefile_cls
        self.packer_cls = packer_cls

    def create(self):
        """
        Create a new store at self.router.root_directory.
        The folder must be empty or not exist.
        """
        if not os.path.exists(self.root):
            os.makedirs(self.root)
        elif os.listdir(self.root):
            raise MyStoreError("Root directory for database is not empty")
        self.dump_config()
        return self

    @classmethod
    def load(cls, root):
        """
        Get an instance representing an existing store.
        """
        config = cls.load_config(root)
        router_cls = _REGISTRY[config["router_cls"]]
        router_params = config["params"]
        router = router_cls(root, router_params)
        return cls(
            router,
            basefile_cls=_REGISTRY[config["basefile_cls"]],
            packer_cls=_REGISTRY[config["packer_cls"]]
        )

    def reader(self, mode="R", threadlock=None):
        """
        Context manager which keeps reference
        """
        return Reader(self, mode, threadlock)

    def writer(self, mode="W", threadlock=None):
        return Writer(self, mode, threadlock)

    def dump_config(self):
        config = {
            "basefile_cls": self.basefile_cls.__name__,
            "packer_cls": self.packer_cls.__name__,
            "router_cls": self.router.__class__.__name__,
            "params": self.router.params
        }
        config_str = json.dumps(config)
        filepath = os.path.join(self.root, CONFIG_FILENAME)
        with open(filepath, "w", encoding="utf8") as f:
            f.write(config_str)

    @classmethod
    def load_config(cls, root_directory):
        # a. Try loading current config format
        filepath = os.path.join(root_directory, CONFIG_FILENAME)
        if os.path.exists(filepath):
            with open(filepath, encoding="utf8") as f:
                config_str = f.read()
            return json.loads(config_str)

        # b. Legacy config version (dbmdb)
        old_filepath = os.path.join(root_directory, DBMDB_FILENAME)
        if os.path.exists(old_filepath):
            with open(old_filepath, encoding="utf8") as f:
                params_str = f.read()
            params = json.loads(params_str)
            return {
                "basefile_cls": DbmFile.__name__,
                "packer_cls": CompressedJsonPacker.__name__,
                "router_cls": OriginalRouter.__name__,
                "params": {
                    "dbm_size": params["dbm_size"],
                    "subfolder_size": params["subfolder_size"],
                    "first_key": 1 if params["version"] == 0 else 0
                }
            }

        raise MyStoreError("No Config File Found")


    def reformat(self, new_db, read_raw=False):
        with new_db.writer("w") as writer:
            with self.reader("r") as reader:
                for k,v in reader.get_all(raw=read_raw):
                    writer[int(k)] = v
    # TODO: this is ugly. Change packers from being hardcoded in configs to some
    #       kind of value handlers? Keys are always integer?
