"""
This module contains Reader and Writer classes,
responsible for reading/writing data to the DB.
"""
import logging
lg = logging.getLogger(__name__)

from .errors import BaseUnitDoesNotExist


class DummyThreadLock:
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class Cursor:
    def __init__(self, db, mode, threadlock=None):
        self.db = db
        self.mode = mode
        self.threadlock = DummyThreadLock() if not threadlock else threadlock
        self._unit = None   # currently opened file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._close_opened_unit()
        delattr(self, "_unit")

    # ******* implementation details *******
    def _close_opened_unit(self):
        if self._unit is not None:
            lg.debug("closing old file handle")
            self._unit.close()
            self._unit = None


class Writer(Cursor):
    def __init__(self, db, mode="W", threadlock=None):
        super().__init__(db, mode, threadlock)

    def __setitem__(self, k, v):
        filepath = self.db.router.get_path(k)
        with self.threadlock:
            if self._unit and (self._unit.path == filepath):
                lg.debug("file already open, skipping")
            else:
                self._close_opened_unit()
                self._unit = self.db.unit_cls(filepath, mode=self.mode)
            self._unit[k] = self.db.converter.dump(v)


class Reader(Cursor):
    def __init__(self, db, mode="W", threadlock=None):
        super().__init__(db, mode, threadlock)

    def __getitem__(self, k):
        unit_path = self.db.router.get_path(k)
        with self.threadlock:
            if self._unit and (self._unit.path == unit_path):
                lg.debug("file already open, skipping")
            else:
                self._close_opened_unit()
                self._unit = self.db.unit_cls(unit_path, mode=self.mode)
            v = self._unit[k]
        return self.db.converter.load(v)

    def get(self, k, default=None):
        """ """
        try:
            return self[k]
        except (BaseUnitDoesNotExist, KeyError):
            return default

    def get_many(self, keys, default=None):
        """
        Get many values at once (to minimize open/close calls) and return as dict.
        Key order is not preserved.
        """
        keys_and_paths = sorted(((k, self.db.router.get_path(k)) for k in keys), key=lambda x:x[1])
        return {k: self.get(k, default) for k, path in keys_and_paths}

    def get_all(self):
        """
        [Generator]
        Return all values:
        go through all files in DB and return all key:value pairs from each file.
        """
        for unit_path in self.db.unit_cls.get_all_unit_paths(self.db.root):
            with self.db.unit_cls(unit_path, mode=self.mode) as f:
                contents = f.items()
                for k,v in contents:
                    yield (k, self.db.converter.load(v))
            lg.debug("unit path read: %s" % unit_path)
