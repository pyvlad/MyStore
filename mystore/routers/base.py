"""
This module contains BaseRouter, abstract base class
responsible for mapping keys to files storing data in DB directory.
"""
import logging
lg = logging.getLogger(__name__)

import os
import abc


class BaseRouter(metaclass=abc.ABCMeta):
    """
    Abstract base class responsible for mapping keys
    to files storing data in DB directory.

    Arguments
    ---------
    root_dir: str
        Base directory where tree of dbm files is stored.
    params: dict
        Dictoinary with other parameters used by the 'get_path' method.
    """
    def __init__(self, root_dir, params):
        self.root_dir = os.path.abspath(os.path.expanduser(root_dir))
        self.params = params

    @abc.abstractmethod
    def get_path(self, key):
        """ Return path to base file with data. """

    @abc.abstractmethod
    def all_filepaths(self):
        """ Generator yielding all files with data. """
