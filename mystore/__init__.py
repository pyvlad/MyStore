"""
Simple key:value store, where data is stored in a tree of files.
"""
from .units import (
    BaseUnit,
    DbmFileUnit,
    JsonFileUnit,
    DirUnit
)
from .converters import (
    FakeConverter,
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
