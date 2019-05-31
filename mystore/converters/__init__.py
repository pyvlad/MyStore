"""
This subpackage contains converter classes nad handler functiions.
- Handler functions can convert values to all kinds of formats used to store them on disk.
- Converter classes define which handler functions are used by a DB
  to convert values from Python objects to DB format and back.
"""
from .base import BaseConverter
from . import handlers
from .classes import (
    CompressedJsonConverter,
    Base64CompressedJsonConverter
)
