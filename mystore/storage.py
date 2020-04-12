"""
Storage based on mystore.DB with sqlite file on top
mapping user provided keys to actual keys used in DB:
- user keys are stored in an indexed column;
- actual keys in DB are automatically incremented integers.

Using this class requires sqlalchemy as an extra dependency.
"""
import logging
lg = logging.getLogger(__name__)

import os
from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base

from .main import DB
from .shortcuts import get_db
from .errors import MyStoreError
from .units import DbmFileUnit
from .routers import OriginalRouter
from .converters import CompressedJsonConverter


SQLITE_FILENAME = "mapping.sqlite3"
Base = declarative_base()


@contextmanager
def session_scope(session):
    """
    Provide a transactional scope around a series of operations.
    """
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class MapRow(Base):
    __tablename__ = "keys"

    pk = sa.Column(sa.Integer, primary_key=True)    # mystore key
    key = sa.Column(sa.Integer, index=True, unique=True, nullable=False)  # actual key

    def __repr__(self):
        return "<MapRow(key=%s at pk=%s)>" % (self.key, self.pk)


class Storage:
    def __init__(self, db_path, params=None, router_cls=OriginalRouter,
                       unit_cls=DbmFileUnit, converter_cls=CompressedJsonConverter,
                       mapping_cls=MapRow):
        """
        Initialize Storage instance.
        Initialize mystore.DB instance (create if not present yet).
        Configure SQLite access (create file if not present yet).
        """
        try:
            self.db = get_db(db_path)
        except MyStoreError:
            if params is None:
                params={
                    "unit_size": 1000,
                    "subfolder_size": 100,
                    "first_key": 1
                }
            self.db = DB(db_path, params, router_cls, unit_cls, converter_cls).create()

        self.mapping_cls = mapping_cls
        self.sqlite_filepath = os.path.join(db_path, SQLITE_FILENAME)
        self.session_factory = self.set_up_sqlite(self.sqlite_filepath)

    def set_up_sqlite(self, filepath):
        """
        Get session factory.
        Create a new SQLite DB or use an existing one (if it already exists).
        """
        db_engine_string = URL(drivername = 'sqlite', database = filepath)
        engine = sa.create_engine(db_engine_string, echo=False)
        # create a session factory:
        # (this factory, when called, will create a new Session object
        #  using the configurational arguments we’ve given the factory)
        session_factory = sessionmaker()
        # provide configurational arguments (an extra step is unneccessary here, but clearer):
        session_factory.configure(bind=engine)
        # create DB if it doesn't exist yet / or does nothing
        # Base.metadata.create_all(engine)
        self.mapping_cls.__table__.create(engine, checkfirst=True)
        # return the factory (passing new arguments when calling
        # session_factory will override existing configuration):
        return session_factory

    def save_one(self, k, v):
        """
        Save one key value pair.
        Doesn't overwrite existing keys.
        """
        mr = self.mapping_cls(key=k)    # mr stands for 'MapRow' object
        with session_scope(self.session_factory()) as session:
            session.add(mr)
            session.flush()
            with self.db.writer() as writer:
                writer[mr.pk] = v

    def save_many(self, items):
        """
        Save many key-value pairs at once.
        Raise an exception and write nothing on duplicate keys.
        """
        mrs = [self.mapping_cls(key=k) for k,v in items]
        with session_scope(self.session_factory()) as session:
            session.add_all(mrs)    # bulk_save_objects?
            session.flush()
            with self.db.writer() as writer:
                for mr, (k,v) in zip(mrs, items):
                    writer[mr.pk] = v

    def save_many_if_missing(self, items):
        """
        Same as the save_many, but won't raise an error if a key is already there.
        Only saves those that aren't in DB yet.
        """
        mrs = [self.mapping_cls(key=k) for k,v in items]
        keys = [k for k,v in items]

        with session_scope(self.session_factory()) as session:
            q = session.query(self.mapping_cls).filter(self.mapping_cls.key.in_(keys))
            keys_in_db = set(mr.key for mr in q)
            missing = [(mr, v) for (k,v), mr in zip(items, mrs) if k not in keys_in_db]

            session.add_all([mr for mr, v in missing])
            session.flush()

            with self.db.writer() as writer:
                for mr, v in missing:
                    writer[mr.pk] = v

    def get_one(self, k, default=None):
        """
        Get one value from storage for specified key.
        """
        with session_scope(self.session_factory()) as session:
            q = session.query(self.mapping_cls).filter(self.mapping_cls.key == k)
            mr = q.one_or_none() # error if multiple results, None if no results
            if mr is None:
                return default
            else:
                with self.db.reader() as reader:
                    v = reader[mr.pk]
                return v

    def get_many(self, keys, default=None):
        """
        Get list of values for given list of keys.
        """
        with session_scope(self.session_factory()) as session:
            q = session.query(self.mapping_cls).filter(self.mapping_cls.key.in_(keys))
            mrs = q.all()
            pk_map = {mr.key: mr.pk for mr in mrs}
            if mrs:
                with self.db.reader() as reader:
                    pkv_dict = reader.get_many([mr.pk for mr in mrs])
            else:
                pkv_dict = {}

        values = []
        for key in keys:
            pk = pk_map.get(key)
            if pk is not None:
                values += [pkv_dict.get(pk, default)]
            else:
                values += [default]

        return values

    def yield_many(self, keys, default=None, chunk_size = 1000):
        """
        Generator to avoid reading all items at once.
        Returns tuples (key, value).
        """
        while True:
            chunk_of_keys, keys = keys[:chunk_size], keys[chunk_size:]
            if not chunk_of_keys:
                return
            values = self.get_many(chunk_of_keys, default)
            for key, value in zip(chunk_of_keys, values):
                yield (key, value)

    def get_missing_keys(self, keys):
        """
        Get list of keys that are not in storage for given list of keys.
        """
        with session_scope(self.session_factory()) as session:
            q = session.query(self.mapping_cls).filter(self.mapping_cls.key.in_(keys))
            keys_in_db = set(mr.key for mr in q)
        return [k for k in keys if k not in keys_in_db]

    def get_all(self, chunk=100):
        """
        Generator.
        Get all (k,v) pairs stored in storage.
        """
        offset = 0
        while True:
            with session_scope(self.session_factory()) as session:
                q = session.query(self.mapping_cls).order_by(self.mapping_cls.pk)
                mrs = q[offset:(offset+chunk)]
                pk_map = {mr.pk: mr.key for mr in mrs}
                if mrs:
                    with self.db.reader() as reader:
                        pkv_dict = reader.get_many([mr.pk for mr in mrs])

            for pk, v in pkv_dict.items():
                yield (pk_map[pk], v)

            offset += len(mrs)
            if len(mrs) < chunk:
                break
