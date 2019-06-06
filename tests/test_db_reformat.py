import unittest
import shutil

from mystore import (
    shortcuts,
    DB,
    OriginalRouter,
    CompressedJsonConverter
)

from tests.helpers import get_db_path


class DBReformatTest(unittest.TestCase):
    def setUp(self):
        self.data = [(i, {"entry_key":i, "value": "some value %s" % str(i)}) for i in range(0,20)]
        self.root1 = get_db_path()
        self.root2 = get_db_path() + "2"
        self.root3 = get_db_path() + "3"
        self.params = {
            "unit_size": 7,
            "subfolder_size": 1,
            "first_key": 0
        }
        self.filedb_params = {
            "digits": 3,
            "subfolder_digits": [1,1]
        }
        self.db = shortcuts.create_dbmdb(
            self.root1,
            self.params["unit_size"],
            self.params["subfolder_size"],
            self.params["first_key"]
        )
        with self.db.writer() as writer:
            for k, v in self.data:
                writer[k] = v

    def tearDown(self):
        shutil.rmtree(self.root1, ignore_errors=True)
        shutil.rmtree(self.root2, ignore_errors=True)
        shutil.rmtree(self.root3, ignore_errors=True)
        self.db = None

    def test_reformat_as_json(self):
        new_db = shortcuts.dbmdb_to_jsondb(self.root1, self.root2)

        expected = sorted(self.data)
        with new_db.reader("r") as reader:
            retrieved = sorted([(int(k), v) for k,v in reader.get_all()])

        self.assertListEqual(retrieved, expected)

    def test_reformat_from_json(self):
        db2 = shortcuts.dbmdb_to_jsondb(self.root1, self.root2)
        db3 = shortcuts.jsondb_to_dbmdb(self.root2, self.root3)

        expected = sorted(self.data)
        with db3.reader("r") as reader:
            retrieved = sorted([(int(k), v) for k,v in reader.get_all()])
        self.assertListEqual(retrieved, expected)


    def test_reformat_as_filedb(self):
        new_db = shortcuts.dbmdb_to_filedb(self.root1, self.root2, self.filedb_params)

        expected = sorted(self.data)
        with new_db.reader("r") as reader:
            retrieved = sorted([(int(k), v) for k,v in reader.get_all()])

        self.assertListEqual(retrieved, expected)

    def test_reformat_from_filedb(self):
        db2 = shortcuts.dbmdb_to_filedb(self.root1, self.root2, self.filedb_params)
        db3 = shortcuts.filedb_to_dbmdb(self.root2, self.root3, self.params)

        expected = sorted(self.data)
        with db3.reader("r") as reader:
            retrieved = sorted([(int(k), v) for k,v in reader.get_all()])
        self.assertListEqual(retrieved, expected)

    def test_reformat_as_leveldb(self):
        from mystore.units import LeveldbUnit

        new_db = DB(self.root2, self.params, OriginalRouter, LeveldbUnit, CompressedJsonConverter)
        new_db.create()
        # monkey patch converters to avoid unneccessary conversions:
        self.db.converter._load_handlers = []
        new_db.converter._dump_handlers = []
        self.db.reformat(new_db)

        expected = sorted(self.data)
        with new_db.reader("r") as reader:
            retrieved = sorted([(int(k), v) for k,v in reader.get_all()])
        self.assertListEqual(retrieved, expected)

        with new_db.reader("r") as reader:
            keys = [k for k,v in self.data]
            retrieved = sorted([(int(k), v) for k,v in reader.get_many(keys).items()])
        self.assertListEqual(retrieved, expected)
