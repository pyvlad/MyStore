import unittest

from mystore import (
    DB,
    MyStoreError
)

from tests.helpers import DBTestsSetup


class DBCreateTest(DBTestsSetup, unittest.TestCase):
    def test_load_existing(self):
        """
        Test 'load' method.
        """
        # initialize an existing DB from serialized configs
        db = DB.load(self.root_dir)

        # assert that it has same configuration attributes
        keys_to_compare = ["root_dir", "unit_size", "subfolder_size", "first_key"]
        params = [[getattr(router, k) for k in keys_to_compare]
                  for router in (self.db.router, db.router)]

        self.assertListEqual(params[0], params[1])

    def test_create_db_nonempty_dir(self):
        """
        Try creating another db in existing non-empty directory.
        """
        with self.assertRaises(MyStoreError):
            db = DB(self.root_dir, self.params)
            db.create()
