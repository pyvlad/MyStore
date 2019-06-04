"""
This subpackage contains BaseFile abstract base class and its implementations.
BaseFile represents basic storage unit used in the package.
"""
from .basefile import BaseFile
from .dbmfile import DbmFile
from .jsonfile import JsonFile
from .dirfile import DirFile
try:
    import plyvel
except ImportError:
    pass
else:
    from .leveldbfile import LeveldbFile
