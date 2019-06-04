"""
This module contains StringFormatRouter,
router class mapping integers to directories of files.
"""
import logging
lg = logging.getLogger(__name__)

import os

from .base import BaseRouter


class StringFormatRouter(BaseRouter):
    """
    BaseRouter subclass mapping integer keys to directory file paths.

    Arguments
    ---------
    root_dir: str
        Base directory where tree of dbm files is stored.
    params: dict
        digits: int
            Minimal number of symbols in key string, e.g. "0002455" if 7.
        subfolder_digits: list of ints
            Number of digits for each subfolder level.

    Example 1:
    >>> router = StringFormatRouter(root_dir="/tmp/", \
            params={"digits": 7, "subfolder_digits":[2,2]}, extension="")
    >>> router.get_path(22)
    '/tmp/00/00'

    Example 2:
    >>> router = StringFormatRouter(root_dir="/tmp/", \
            params={"digits": 7, "subfolder_digits":[2,2]}, extension="")
    >>> router.get_path(1234567)
    '/tmp/12/34'

    Example 3:
    >>> router = StringFormatRouter(root_dir="/tmp/", \
            params={"digits": 7, "subfolder_digits":[2,2]}, extension="")
    >>> router.get_path(123456789)
    '/tmp/1234/56'

    Example 4:
    >>> router = StringFormatRouter(root_dir="/tmp/", \
            params={"digits": 2, "subfolder_digits":[]}, extension="")
    >>> router.get_path(12)
    '/tmp/data'
    """
    def __init__(self, root_dir, params, extension):
        super().__init__(root_dir, params, extension)
        self.digits = params["digits"]                      # e.g. 7
        self.subfolder_digits = params["subfolder_digits"]  # e.g. [2, 2]
        self.cums = [sum(self.subfolder_digits[:(i+1)])
                     for i in range(len(self.subfolder_digits))] # e.g. [2, 4]
        self.idxs = [i - self.digits for i in self.cums]    # e.g. [-5, -3]
        self.str_key_template = "%0{}d".format(self.digits) # e.g."%07d"

    def get_path(self, key):
        """
        Returns dir path derived from integer key.
        """
        str_key = self.str_key_template % key
        if self.idxs:
            subfolders = [str_key[:self.idxs[0]]]           # e.g. "0123456"[:-5], or "01"
            for i,j in zip(self.idxs, self.idxs[1:]):
                subfolders += [str_key[i:j]]
        else:
            subfolders = ["data"]
        path = os.path.join(self.root_dir, *subfolders)
        return path + self.extension

if __name__ == "__main__":
    import doctest
    doctest.testmod()
