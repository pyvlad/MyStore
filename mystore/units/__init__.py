"""
This subpackage contains BaseUnit abstract base class and its implementations.
BaseUnit represents basic storage unit used in the package.
"""
from .base import BaseUnit
from .dbmfile import DbmFileUnit
from .jsonfile import JsonFileUnit
from .dir import DirUnit
try:
    import plyvel
except ImportError:
    pass
else:
    from .leveldb import LeveldbUnit
