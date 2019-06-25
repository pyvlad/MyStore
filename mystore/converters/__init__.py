"""
This subpackage contains converter classes and handler functiions:
- Handlers are out-of-the-box functions that can convert values
  to all kinds of formats used to store them on disk.
- Converters define which handler functions are used by a DB
  to convert values from Python objects to DB format and back.
"""
from .base import BaseConverter
from . import handlers
from .classes import (
    FakeConverter,
    CompressedJsonConverter,
    Base64CompressedJsonConverter
)
