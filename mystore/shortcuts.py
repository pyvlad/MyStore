"""
Shortcuts functions for common use and those that replicate
functionality of the previous library version.
"""
import logging
lg = logging.getLogger(__name__)


from .main import DB
from .routers import (
    OriginalRouter,
    JsonRouter,
    DirRouter
)
from .basefiles import (
    DbmFile,
    JsonFile,
    DirFile
)
from .converters import (
    CompressedJsonConverter,
    Base64CompressedJsonConverter,
    handlers
)


def get_db(root):
    """ Load DB object for an existing db. """
    return DB.load(root)


def create_dbmdb(root, dbm_size, subfolder_size, first_key=1):
    """ Create dbmdb: default DB format. """
    router = OriginalRouter(
        root,
        params={
            "unit_size": dbm_size,
            "subfolder_size": subfolder_size,
            "first_key": first_key
        }
    )
    return DB(router, basefile_cls=DbmFile, converter_cls=CompressedJsonConverter).create()


def dbmdb_to_jsondb(old_path, new_path):
    """ Reformat existing dbmdb to db with JSON files instead of gdbm files. """
    old_db = get_db(old_path)
    router = JsonRouter(new_path, params=old_db.router.params)
    new_db = DB(router, JsonFile, Base64CompressedJsonConverter).create()
    # monkey patch converters to avoid unneccessary conversions:
    old_db.converter._load_handlers = []
    new_db.converter._dump_handlers = [handlers.bytes_to_base64_string]
    old_db.reformat(new_db)
    return new_db


def jsondb_to_dbmdb(old_path, new_path):
    """ Reformat existing db with JSON files into a dbmdb with gdbm files. """
    old_db = get_db(old_path)
    router = OriginalRouter(new_path, params=old_db.router.params)
    new_db = DB(router, DbmFile, CompressedJsonConverter).create()
    # monkey patch converters to avoid unneccessary conversions:
    old_db.converter._load_handlers = [handlers.bytes_from_base64_string] # read bytes
    new_db.converter._dump_handlers = [] # write bytes
    old_db.reformat(new_db)
    return new_db


def dbmdb_to_filedb(old_path, new_path, new_router_params):
    """ Reformat existing dbmdb to db with plain files instead of gdbm files. """
    old_db = get_db(old_path)
    router = DirRouter(new_path, params=new_router_params)
    new_db = DB(router, DirFile, CompressedJsonConverter).create()
    # monkey patch converters to avoid unneccessary conversions:
    old_db.converter._load_handlers = []
    new_db.converter._dump_handlers = []
    old_db.reformat(new_db)
    return new_db


def filedb_to_dbmdb(old_path, new_path, new_router_params):
    """ Reformat existing filedb to dbmdb with plain files instead of gdbm files. """
    old_db = get_db(old_path)
    router = OriginalRouter(new_path, params=new_router_params)
    new_db = DB(router, DbmFile, CompressedJsonConverter).create()
    # monkey patch converters to avoid unneccessary conversions:
    old_db.converter._load_handlers = [] # read bytes
    new_db.converter._dump_handlers = [] # write bytes
    old_db.reformat(new_db)
    return new_db
