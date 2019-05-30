"""
This module contains Writer, a class responsible for write operations.
"""
import logging
lg = logging.getLogger(__name__)

import os
import sys
import dbm
import time

from .dbmfile import JsonDbmFile
from .misc import DummyThreadlock


class Writer:
    """
    Class responsible for write operations.

    Arguments
    ---------
    router: obj
        dbmdb.Router object to route a key to a dbm file
    threadlock: obj
        pass threadlock to synchronize multiple writers
        and make write operations thread safe
    """
    WAIT_TIME = 1       # wait time to save value if dbmfile is in use
    _cache = []

    def __init__(self, router, threadlock=None):
        self.router = router
        self.threadlock = DummyThreadlock() if not threadlock else threadlock
        self.file = None    # currently open dbm file

    def save_value(self, key, value):
        """
        Arguments
        ---------
        key: int
            Integer key.
        value: obj / bytes
            The value must be either:
            1. a JSON serializable Python object, or
            2. a compressed serialized JSON.
        """
        lg.debug("saving value for key %s", key)
        filepath = self.router.get_path_to_dbm(key)

        if self.file is None or self.file.path != filepath:
            lg.debug("closing old dbm file, opening new one")
            self._close_file()
            self._open_file(filepath)
        else:
            lg.debug("dbm already open, skipping")

        self.file[str(key)] = value
        lg.debug("saved value for key %s", key)

    def close(self):
        """
        Close writer instance:
        (a) close currently opened dbm file;
        (b) delete "file" and 'threadlock' attributes
            to fail on attempted "save_value" methods.
        """
        self._close_file()
        delattr(self, "file")
        delattr(self, "threadlock")

    def _close_file(self):
        """
        Close the dbm file associated with instance and clean up.
        """
        if self.file is not None:
            filepath = self.file.path
            lg.debug("closing dbm file (%s)", filepath)
            if filepath is not None:
                self.file.close()               # actual closing call
                with self.threadlock:           # delete from cache of opened files
                    index = Writer._cache.index(filepath)
                    Writer._cache.pop(index)
            lg.debug("closed dbm file (%s)", filepath)

    def _open_file(self, dbmpath):
        """
        Open a dbm file at 'dbmpath' for write operations.
        Store filepath in cache: Writer._cache.
        Try to open the dbm:
        - create directories if necessary;
        - wait if it's in use by another instance or process;
        - retry every WAIT_TIME seconds;
        """
        lg.debug("opening dbm file %s", dbmpath)

        # infinite loop; exits only on success
        while True:
            try:
                with self.threadlock:
                    # check if file path is used by another instance
                    if dbmpath in Writer._cache:
                        error = dbm.error[0]("Dbm file in use by another instance")
                        # dbm.error: (<class 'dbm.error'>, <class 'OSError'>)
                        error.errno = 100
                        raise error
                    # if not: try opening it
                    self.file = JsonDbmFile(dbmpath)
                    # on success: add to cache
                    Writer._cache += [dbmpath]

            except dbm.error:
                # get exception info, a tuple (exc_type, exc_val, exc_tb), e.g.:
                #   (<class '_gdbm.error'>,
                #    error(2, 'No such file or directory'),
                #    <traceback object at 0x7fdedf8d3d08>)
                exc_info = sys.exc_info()
                error = exc_info[1]
                if error.errno == 2:            # error(2, 'No such file or directory')
                    dirname = os.path.dirname(dbmpath)
                    lg.debug("Directory %s does not exist", dirname)
                    if not os.path.exists(dirname):
                        try:
                            with self.threadlock:
                                os.makedirs(dirname)
                            lg.debug("Created directory [%s]", dirname)
                        except FileExistsError:     # created by another process
                            lg.debug("Directory already exists [%s]", dirname)
                        continue
                elif error.errno == 11:     # [Errno 11] Resource temporarily unavailable
                    lg.debug("Resource %s temporarily unavailable (for process)", dbmpath)
                elif error.errno == 100:    # file is opened by another instance (see above)
                    lg.debug("Resource %s temporarily unavailable (for instance)", dbmpath)
                else:                   # other cases
                    lg.error(error)

                # log and wait before retrying
                lg.debug("waiting %s seconds, then re-try", self.WAIT_TIME)
                time.sleep(self.WAIT_TIME)

            else:
                # on success: break the infinite loop
                break

        lg.debug("opened dbm file %s", dbmpath)
