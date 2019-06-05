import os
import time
import random
import tempfile
import shutil

from mystore import DB


def get_db_path():
    return os.path.join(
        tempfile.gettempdir(),
        "mystore_%s_%s" % (time.time(), random.randint(0, 10000))
    )


class DBTestsSetup:
    def setUp(self):
        self.data = [(i, {"entry_key":i, "value": "some value %s" % str(i)}) for i in range(0,10)]
        self.root_dir = get_db_path()
        self.params = {
            "unit_size": 3,
            "subfolder_size": 1,
            "first_key": 0
        }
        self.db = DB(self.root_dir, self.params).create()
        with self.db.writer() as writer:
            for k, v in self.data:
                writer[k] = v

    def tearDown(self):
        shutil.rmtree(self.root_dir, ignore_errors=True)
        self.db = None
