"""
Simple key:value store, where data is stored in a tree of files.
"""
from .basefiles import (
    BaseFile,
    DbmFile,
    JsonFile,
    DirFile
)
from .converters import (
    CompressedJsonConverter,
    Base64CompressedJsonConverter,
    handlers
)
from .routers import (
    BaseRouter,
    OriginalRouter,
    StringFormatRouter
)
from .main import DB
from .errors import MyStoreError
from .shortcuts import *
