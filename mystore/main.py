"""
This module contains the DB class.
This is the main class of the package, it represents a database.
"""
import logging
lg = logging.getLogger(__name__)

import os
import sys
import json

from .units import BaseUnit, DbmFileUnit
from .routers import BaseRouter, OriginalRouter
from .converters import BaseConverter, CompressedJsonConverter as CJC
from .cursors import Reader, Writer
from .errors import MyStoreError


DBMDB_FILENAME = ".dbmdb.json"
CONFIG_FILENAME = "mystore_config"


class DB:
    """
    Main class which represents the key:value store.
    """
    def __init__(self, root, params, router_cls=OriginalRouter,
                 unit_cls=DbmFileUnit, converter_cls=CJC):
        self.root = root
        self.params = params
        self.unit_cls = unit_cls
        self.router = router_cls(root, params, unit_cls.EXTENSION)
        self.converter = converter_cls()

    def create(self):
        """
        Create a new store at self.router.root_dir.
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
        return cls(
            root,
            config["params"],
            router_cls=cls.get_router_classes()[config["router_cls"]],
            unit_cls=cls.get_unit_classes()[config["unit_cls"]],
            converter_cls=cls.get_converter_classes()[config["converter_cls"]]
        )

    def reader(self, mode="R", threadlock=None):
        return Reader(self, mode, threadlock)

    def writer(self, mode="W", threadlock=None):
        return Writer(self, mode, threadlock)

    def dump_config(self):
        config = {
            "unit_cls": self.unit_cls.__name__,
            "converter_cls": self.converter.__class__.__name__,
            "router_cls": self.router.__class__.__name__,
            "params": self.router.params
        }
        config_str = json.dumps(config)
        filepath = os.path.join(self.root, CONFIG_FILENAME)
        with open(filepath, "w", encoding="utf8") as f:
            f.write(config_str)

    @classmethod
    def load_config(cls, root_dir):
        # a. Try loading current config format
        filepath = os.path.join(root_dir, CONFIG_FILENAME)
        if os.path.exists(filepath):
            with open(filepath, encoding="utf8") as f:
                config_str = f.read()
            return json.loads(config_str)

        # b. Legacy config version (dbmdb)
        old_filepath = os.path.join(root_dir, DBMDB_FILENAME)
        if os.path.exists(old_filepath):
            with open(old_filepath, encoding="utf8") as f:
                params_str = f.read()
            params = json.loads(params_str)
            return {
                "unit_cls": DbmFileUnit.__name__,
                "converter_cls": CJC.__name__,
                "router_cls": OriginalRouter.__name__,
                "params": {
                    "unit_size": params["dbm_size"],
                    "subfolder_size": params["subfolder_size"],
                    "first_key": 1 if params["version"] == 0 else 0
                }
            }

        raise MyStoreError("No Config File Found")


    def reformat(self, new_db):
        with new_db.writer("w") as writer:
            with self.reader("r") as reader:
                for k,v in reader.get_all():
                    writer[int(k)] = v
    # TODO: Keys are always integer?

    @staticmethod
    def get_unit_classes():
        return {cls.__name__: cls for cls in BaseUnit.__subclasses__()}

    @staticmethod
    def get_router_classes():
        return {cls.__name__: cls for cls in BaseRouter.__subclasses__()}

    @staticmethod
    def get_converter_classes():
        return {cls.__name__: cls for cls in BaseConverter.__subclasses__()}
