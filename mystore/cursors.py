"""
This module contains Reader and Writer classes, responsible
for reading/writing data to the DB.
"""
import logging
lg = logging.getLogger(__name__)


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
        self._file = None   # currently opened file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._close_opened_file()
        delattr(self, "_file")

    # ******* implementation details *******
    def _close_opened_file(self):
        if self._file is not None:
            lg.debug("closing old file handle")
            self._file.close()
            self._file = None


class Writer(Cursor):
    def __init__(self, db, mode="W", threadlock=None):
        super().__init__(db, mode, threadlock)

    def __setitem__(self, k, v):
        filepath = self.db.router.get_path(k)
        with self.threadlock:
            if self._file and (self._file.path == filepath):
                lg.debug("file already open, skipping")
            else:
                self._close_opened_file()
                self._file = self.db.basefile_cls(filepath, mode=self.mode)
            self._file[k] = self.db.packer_cls.pack_value(v)


class Reader(Cursor):
    def __init__(self, db, mode="W", threadlock=None):
        super().__init__(db, mode, threadlock)

    def __getitem__(self, k):
        return self.get(k, raw=False)

    def get(self, k, raw=False):
        filepath = self.db.router.get_path(k)
        with self.threadlock:
            if self._file and (self._file.path == filepath):
                lg.debug("file already open, skipping")
            else:
                self._close_opened_file()
                self._file = self.db.basefile_cls(filepath, mode=self.mode)
            v = self._file[k]
            if raw:
                return v
        return self.db.packer_cls.unpack_value(v)

    def get_many(self, keys, raw=False):
        """
        Get many values at once (to minimize open/close calls) and return as dict.
        Key order is not preserved.
        """
        keys_and_paths = sorted(((k, self.db.router.get_path(k)) for k in keys), key=lambda x:x[1])
        data = {}
        for k, path in keys_and_paths:
            data[k] = self.get(k, raw=raw)
        return data

    def get_all(self, raw=False):
        """
        [Generator]
        Return all values:
        go through all files in DB and return all key:value pairs from each file.
        """
        for filepath in self.db.router.all_filepaths():
            with self.db.basefile_cls(filepath, mode=self.mode) as f:
                contents = f.items()
                for k,v in contents:
                    yield (k, v if raw else self.db.packer_cls.unpack_value(v))
