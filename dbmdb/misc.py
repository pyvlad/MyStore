import logging
lg = logging.getLogger(__name__)


class DbmdbError(OSError):
    pass


class DummyThreadlock:
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        return
