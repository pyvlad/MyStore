"""
This module contains BaseRouter, abstract base class
responsible for mapping keys to base units storing data in DB directory.
"""
import logging
lg = logging.getLogger(__name__)

import os
import abc


class BaseRouter(metaclass=abc.ABCMeta):
    """
    Abstract base class responsible for mapping keys
    to base units storing data in DB directory.

    Arguments
    ---------
    root_dir: str
        Base directory where tree of base units is stored.
    params: dict
        Dictoinary with other parameters used by the 'get_path' method.
    extension: str
        Base unit extension, e.g. ".dbm".
    """
    def __init__(self, root_dir, params, extension):
        self.root_dir = os.path.abspath(os.path.expanduser(root_dir))
        self.params = params
        self.extension = extension

    @abc.abstractmethod
    def get_path(self, key):
        """ Return path to base unit with data. """
