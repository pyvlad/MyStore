"""
Simple key:value store, where data is stored in a tree of files.
"""
from .basefiles import (
    BaseFile,
    DbmFile,
    JsonFile
)
from .converters import (
    CompressedJsonConverter,
    Base64CompressedJsonConverter,
    handlers
)
from .routers import (
    BaseRouter,
    OriginalRouter,
    JsonRouter
)
from .main import DB, MyStoreError


# ***********************
#       Shortcuts
# ***********************
def get(root):
    return DB.load(root)

def create_dbmdb(root, dbm_size, subfolder_size, first_key=1):
    router = Router(root, dbm_size, subfolder_size, first_key)
    return DB(router, basefile_cls=DbmFile, packer_cls=CompressedJsonConverter).create()
