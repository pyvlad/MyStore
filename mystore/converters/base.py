"""
This module contains BaseConverter, base class responsible
for converting values into and from the format used to store them on disk.
"""
import logging
lg = logging.getLogger(__name__)


class BaseConverter:
    """
    Represents a class responsible for converting values
    into and from the format used to store them on disk.
    Subclasses only need to provide default lists of handler functions.
    """
    DUMP_HANDLERS = []
    LOAD_HANDLERS = []

    def __init__(self, dump_handlers=None, load_handlers=None):
        """
        Pass lists of handlers as argument for one-time usage.
        Normally should be None, and lists of handlers defined is
        DUMP_HANLDERS and LOAD_HANDLERS lists of each subclass.
        """
        if dump_handlers is None:
            self._dump_handlers = self.__class__.DUMP_HANDLERS
        if load_handlers is None:
            self._load_handlers = self.__class__.LOAD_HANDLERS

    def dump(self, v):
        """ Convert Python object to format ready to be written to DB. """
        for handler in self._dump_handlers:
            v = handler(v)
        return v

    def load(self, v):
        """ Convert value from format stored in DB to Python object. """
        for handler in self._load_handlers:
            v = handler(v)
        return v
