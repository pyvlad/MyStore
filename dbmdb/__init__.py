from .misc import DbmdbError, DummyThreadlock
from .dbmfile import JsonDbmFile
from .router import Router
from .reader import Reader
from .writer import Writer
from .main import Dbmdb


###### Shortcuts #######
def get_db(root_directory):
    return Dbmdb.from_folder(root_directory)

def create_db(root_directory, dbm_size, subfolder_size, first_key=1):
    router = Router(root_directory, dbm_size, subfolder_size, first_key)
    db = Dbmdb(router)
    db.create()
    return db
