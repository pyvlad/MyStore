"""
This subpackage contains router classes,
i.e. classes responsible for mapping keys to subpaths
at which the values are stored.
"""
from .base import BaseRouter
from .original import OriginalRouter
from .stringformat import StringFormatRouter
