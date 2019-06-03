"""
Custom package exceptions go here.
"""
import logging
lg = logging.getLogger(__name__)


class MyStoreError(Exception):
    pass
