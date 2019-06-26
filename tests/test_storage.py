import tempfile
import unittest
import shutil
import sqlalchemy as sa
from mystore.storage import Storage

from tests.helpers import get_db_path


class StorageTest(unittest.TestCase):
    """
    Test Storage class.
    """
    def setUp(self):
        self.first_id = 1
        self.last_id = 10
        self.data = {k: {"id": k, "squared": k**2} for k in range(self.first_id, self.last_id)}
        self.db_path = get_db_path()
        self.params = {
            "unit_size": 3,
            "subfolder_size": 1,
            "first_key": 1
        }
        self.s = Storage(self.db_path, params=self.params)

    def tearDown(self):
        shutil.rmtree(self.db_path, ignore_errors=True)
        self.db = None

    def test_reuse_db(self):
        s2 = Storage(self.db_path, params=None)
        self.assertDictEqual(self.params, s2.db.params) # uses old db params

    def test_add_value(self):
        v0 = self.s.get_one(1)
        self.assertEqual(v0, None)

        # save second item:
        self.s.save_one(2, self.data[2])
        v = self.s.get_one(2)
        with self.s.db.reader() as reader:
            v1 = reader[1]
        self.assertEqual(2, v["id"])
        self.assertEqual(4, v["squared"])
        self.assertEqual(v["id"], v1["id"])

        # now save first item
        self.s.save_one(1, self.data[1])
        v1 = self.s.get_one(1)
        v2 = self.s.get_one(2)
        self.assertEqual(1, v1["id"])
        self.assertEqual(1, v1["squared"])
        self.assertEqual(2, v2["id"])

        # pk should be 2 for 1st element:
        with self.s.db.reader() as reader:
            v = reader[2]
        self.assertEqual(v1["id"], v["id"])

    def test_duplicate_value(self):
        # try saving value second time:
        self.s.save_one(1, self.data[1])
        with self.assertRaises(sa.exc.IntegrityError):
            self.s.save_one(1, self.data[1])
        self.assertEqual(1, self.s.get_one(1)["id"])

    def test_many_items_with_duplicates(self):
        self.s.save_one(1, self.data[1])
        # now try saving all items
        with self.assertRaises(sa.exc.IntegrityError):
            self.s.save_many(list(self.data.items()))

        v = self.s.get_one(1)
        self.assertEqual(1, v["id"])

        # 2nd PK shouldn't be in DB yet
        with self.s.db.reader() as reader:
            v = reader.get(2)
        self.assertEqual(v, None)

        pairs = list(self.s.get_all())
        self.assertEqual(1, len(pairs))  # correct number of items
        self.assertEqual(1, pairs[0][0]) # key

    def test_save_many_if_missing(self):
        # save a couple values first
        self.s.save_one(2, self.data[2])
        self.s.save_one(6, self.data[6])

        self.s.save_many_if_missing(list(self.data.items()))
        pairs = list(self.s.get_all())
        self.assertEqual(len(self.data), len(pairs))  # correct number of items
        saved_data = {k: v for k,v in pairs}
        self.assertDictEqual(saved_data, self.data)

        # try saving again (shouldn't save anything)
        self.s.save_many_if_missing(list(self.data.items()))
        pairs = list(self.s.get_all())
        self.assertEqual(len(self.data), len(pairs))  # correct number of items
        saved_data = {k: v for k,v in pairs}
        self.assertDictEqual(saved_data, self.data)

        # check that number of PKs is correct
        with self.s.db.reader() as reader:
            v = reader.get(len(self.data) + 1)
        self.assertEqual(v, None)

        # check that empty list works
        self.s.save_many_if_missing([])


    def test_get_many(self):
        self.s.save_many(list(self.data.items()))
        keys = [0,1,100]
        expected_ids = [None, 1, None]
        values = [v["id"] if v is not None else v for v in self.s.get_many(keys)]
        self.assertEqual(expected_ids, values)

        keys = [0,100]
        expected_ids = [None, None]
        values = self.s.get_many(keys)
        self.assertEqual(expected_ids, values)


    def test_get_missing(self):
        self.s.save_many(list(self.data.items()))
        keys = [0,1,9,100]
        missing_keys = self.s.get_missing_keys(keys)
        self.assertEqual([0,100], sorted(missing_keys))
