"""
Custom package exceptions go here.
"""
import logging
lg = logging.getLogger(__name__)


class MyStoreError(Exception):
    pass

class BaseUnitDoesNotExist(MyStoreError):
    """
    Raised when a reader tries to access a file that doesn't exist.
    """
    pass
