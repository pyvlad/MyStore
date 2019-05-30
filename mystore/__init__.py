"""
Simple key:value store, where data is stored in a tree of files.
"""
from .basefile import DbmFile
from .packers import CompressedJsonPacker
from .routers import BaseRouter, OriginalRouter
from .main import DB, MyStoreError


# ***********************
#       Shortcuts
# ***********************
def get(root):
    return DB.load(root)

def create_dbmdb(root, dbm_size, subfolder_size, first_key=1):
    router = Router(root, dbm_size, subfolder_size, first_key)
    return DB(router, basefile_cls=DbmFile, packer_cls=CompressedJsonPacker).create()
