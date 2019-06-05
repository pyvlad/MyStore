import unittest
import os
import tempfile

from mystore import (
    DbmFile,
    CompressedJsonConverter,
    MyStoreError
)


class DbmFileTest(unittest.TestCase):
    """
    Test DbmFile base unit.
    """
    @classmethod
    def setUpClass(cls):
        filepath = os.path.join(tempfile.gettempdir(), "temp_dbm.dbm")
        testdata = {
            0: {"name": "zero"},
            1: {"name": "one"},
            10: {"name": "ten"},
            99: {"name": "ninety nine"},
            100: None,
        }
        cls.converter = CompressedJsonConverter()
        with DbmFile(filepath, "w") as f:
            for k, v in testdata.items():
                f[k] = cls.converter.dump(v)
        cls._filepath = filepath
        cls._testdata = testdata

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls._filepath):
            os.remove(cls._filepath)

    # setup and teardown for each test
    def setUp(self):
        self.dbmfile = DbmFile(self._filepath, mode="r")

    def tearDown(self):
        self.dbmfile.close()

    # tests for methods
    def test_str(self):
        name = str(self.dbmfile)
        self.assertEqual(name, self._filepath)

    def test_get(self):
        keys = [1, 100]
        testdata_values = [self._testdata[key] for key in keys]
        values = [self.converter.load(self.dbmfile[key]) for key in keys]
        self.assertEqual(values, testdata_values)

    def test_get_raw(self):
        keys = [1, 100]
        testdata_values = [self.converter.dump(self._testdata[key])[8:] for key in keys]
        # bytes 4-8 are for timestamp:
        # http://www.onicos.com/staff/iz/formats/gzip.html
        values = [self.dbmfile[key][8:] for key in keys]
        self.assertEqual(values, testdata_values)

    def test_keys(self):
        self.assertEqual(
            sorted([int(k) for k in self.dbmfile.keys()]),
            sorted(self._testdata.keys())
        )

    def test_get_all_items(self):
        all_values = {int(k): self.converter.load(v) for k, v in self.dbmfile.items()}
        self.assertDictEqual(all_values, self._testdata)

    def test_unsupported_mode(self):
        with self.assertRaises(MyStoreError):
            dbmfile = DbmFile(self._filepath, mode="yo")
