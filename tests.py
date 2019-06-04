"""
Run unittests:
python tests.py -v
"""
import unittest
import os
import sys
import tempfile
import time
import shutil
import random

from mystore import DB
from mystore import DbmFile, JsonFile
from mystore import OriginalRouter, JsonRouter
from mystore import MyStoreError
from mystore import CompressedJsonConverter as CJC, Base64CompressedJsonConverter
from mystore import handlers
from mystore import shortcuts
from mystore import errors


def get_db_path():
    return os.path.join(
        tempfile.gettempdir(),
        "mystore_%s_%s" % (time.time(), random.randint(0, 10000))
    )


class OriginalRouterTest(unittest.TestCase):
    """
    Tests for all things create/initialize DB
    """
    def setUp(self):
        self.root_dir = get_db_path()
        os.makedirs(self.root_dir)

        sep = os.path.sep
        ext = ".dbm"
        self.data = [
            # unit_size, subfolder_size, key, expected_filename(v1), expected filename(v0)
            (1, 0, 0, '0%s'%ext, '-1%s'%(ext)),
            (1, 0, 1, '1%s'%ext, '0%s'%(ext)),
            (1, 0, 7, '7%s'%ext, '6%s'%(ext)),
            (1, 0, 10, '10%s'%ext, '9%s'%(ext)),

            (1, 1, 0, '0%s0%s'%(sep,ext), '-1%s0%s'%(sep,ext)),
            (1, 1, 1, '1%s0%s'%(sep,ext), '0%s0%s'%(sep,ext)),
            (1, 1, 7, '7%s0%s'%(sep,ext), '6%s0%s'%(sep,ext)),
            (1, 1, 10, '10%s0%s'%(sep,ext), '9%s0%s'%(sep,ext)),

            (1, 2, 0, '0%s0%s'%(sep,ext), '-1%s1%s'%(sep,ext)),
            (1, 2, 1, '0%s1%s'%(sep,ext), '0%s0%s'%(sep,ext)),
            (1, 2, 7, '3%s1%s'%(sep,ext), '3%s0%s'%(sep,ext)),
            (1, 2, 10, '5%s0%s'%(sep,ext), '4%s1%s'%(sep,ext)),

            (2, 0, 0, '0%s'%(ext), '-1%s'%(ext)),
            (2, 0, 1, '0%s'%(ext), '0%s'%(ext)),
            (2, 0, 7, '3%s'%(ext), '3%s'%(ext)),
            (2, 0, 10, '5%s'%(ext), '4%s'%(ext)),

            (2, 1, 0, '0%s0%s'%(sep,ext), '-1%s0%s'%(sep,ext)),
            (2, 1, 1, '0%s0%s'%(sep,ext), '0%s0%s'%(sep,ext)),
            (2, 1, 7, '3%s0%s'%(sep,ext), '3%s0%s'%(sep,ext)),
            (2, 1, 10, '5%s0%s'%(sep,ext), '4%s0%s'%(sep,ext)),

            (2, 2, 0, '0%s0%s'%(sep,ext), '-1%s1%s'%(sep,ext)),
            (2, 2, 1, '0%s0%s'%(sep,ext), '0%s0%s'%(sep,ext)),
            (2, 2, 7, '1%s1%s'%(sep,ext), '1%s1%s'%(sep,ext)),
            (2, 2, 10, '2%s1%s'%(sep,ext), '2%s0%s'%(sep,ext)),
        ]

    def tearDown(self):
        shutil.rmtree(self.root_dir, ignore_errors=True)

    def test_path_retrieval_fk0(self):
        """
        Test function converting generating path to a dbm file given
        (a) configuration attributes and
        (b) key value
        """
        params = lambda row: {
            "unit_size": row[0],
            "subfolder_size": row[1],
            "first_key": 0
        }
        routers = [OriginalRouter(self.root_dir, params(row)) for row in self.data]
        expected_values = [os.path.join(self.root_dir, row[3]) for row in self.data]
        returned_values = [router.get_path(row[2]) for router, row in zip(routers, self.data)]
        self.assertListEqual(returned_values, expected_values)

    def test_path_retrieval_fk1(self):
        """
        Test an older version which assumed that keys start from 1
        """
        params = lambda row: {
            "unit_size": row[0],
            "subfolder_size": row[1],
            "first_key": 1
        }
        routers = [OriginalRouter(self.root_dir, params(row)) for row in self.data]
        expected_values = [os.path.join(self.root_dir, row[4]) for row in self.data]
        returned_values = [router.get_path(row[2]) for router, row in zip(routers, self.data)]
        self.assertListEqual(returned_values, expected_values)


class DbmFileTest(unittest.TestCase):
    """
    Test JSONDbmFile wrapper class.
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
        cls.converter = CJC()
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


class DBTestsSetup:
    def setUp(self):
        self.data = [(i, {"entry_key":i, "value": "some value %s" % str(i)}) for i in range(0,10)]
        self.root_dir = get_db_path()
        self.params = {
            "unit_size": 3,
            "subfolder_size": 1,
            "first_key": 0
        }
        self.router = OriginalRouter(self.root_dir, params=self.params)
        self.db = DB(self.router)
        self.db.create()

        with self.db.writer() as writer:
            for k, v in self.data:
                writer[k] = v

    def tearDown(self):
        shutil.rmtree(self.root_dir, ignore_errors=True)
        self.db = None


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
            db = DB(self.router)
            db.create()


class ReaderTest(DBTestsSetup, unittest.TestCase):
    def test_get_value(self):
        """ """
        with self.db.reader() as reader:
            retrieved_data = [(k, reader[k]) for k, v in self.data]
        self.assertListEqual(retrieved_data, self.data)

    def test_getitem_nonexisting_file_r(self):
        """ """
        with self.assertRaises(errors.BaseUnitDoesNotExist):
            with self.db.reader("r") as reader:
                retrieved_value = reader[10*10]

    def test_getitem_nonexisting_file_R(self):
        """ """
        with self.assertRaises(errors.BaseUnitDoesNotExist):
            with self.db.reader("R") as reader:
                retrieved_value = reader[10*10]

    def test_get_value_concurrent(self):
        """ """
        retrieved_data = []

        def save_in_thread(root_dir, keys_to_read, lock):
            nonlocal retrieved_data
            with self.db.reader(threadlock=lock) as reader:
                for k in keys_to_read:
                    v = reader[k]
                    with lock:
                        retrieved_data += [(k, v)]

        import threading
        threads_number = 5
        lock = threading.Lock()
        coef = len(self.data) // threads_number
        threads = [
            threading.Thread(
                target=save_in_thread,
                args=[
                    self.root_dir,
                    [k for k,v in self.data[(i*coef):((i+1)*coef)]],
                    lock
                ]
            )
            for i in range(threads_number)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        expected = sorted(self.data)
        retrieved = sorted(retrieved_data)

        self.assertListEqual(retrieved, expected)

    def test_get_many(self):
        """ """
        keys = [k for k, v in self.data]
        with self.db.reader() as reader:
            retrieved_data = reader.get_many(keys=keys)
            retrieved = sorted(retrieved_data.items())
        expected = sorted(self.data)
        self.assertListEqual(retrieved, expected)

    def test_get_many_with_missing(self):
        """ """
        keys = [k for k, v in self.data]
        extra_keys = [max(keys) + 1, -1000, 1000]
        keys += extra_keys
        with self.db.reader() as reader:
            retrieved_data = reader.get_many(keys=keys)
            retrieved = sorted(retrieved_data.items())
        expected = self.data + [(k, None) for k in extra_keys]
        expected = sorted(expected)
        self.assertListEqual(retrieved, expected)

    def test_get_all(self):
        """ """
        with self.db.reader() as reader:
            retrieved = sorted((int(k), v) for k,v in reader.get_all())
        expected = sorted(self.data)
        self.assertListEqual(retrieved, expected)


class DBConcurrencyTest(unittest.TestCase):
    """
    Tests to check correctness of concurrent saving from different instances.
    """
    def setUp(self):
        self.data = [(i, {"entry_key":i, "value": "some value %s" % str(i)}) for i in range(0,10)]
        self.root_dir = get_db_path()
        self.params = {
            "unit_size": 3,
            "subfolder_size": 1,
            "first_key": 0
        }
        self.router = OriginalRouter(self.root_dir, params=self.params)
        self.db = DB(self.router)
        self.db.create()

        self.threads_number = 5
        self.processes_number = 5

    def tearDown(self):
        shutil.rmtree(self.root_dir, ignore_errors=True)
        self.db = None

    #@unittest.skip("skipping checking threads")
    def test_save_in_threads(self):
        def save_in_thread(db, data_to_save, lock):
            """ helper function to run in thread """
            with db.writer() as writer:
                for k, v in data_to_save:
                    writer[k] = v

        import threading
        lock = threading.Lock()
        coef = len(self.data) // self.threads_number
        threads = [
            threading.Thread(
                target=save_in_thread,
                args=[self.db, self.data, lock]
            )
            for i in range(self.threads_number)
        ]
        for thread in threads: thread.start()
        for thread in threads: thread.join()

        expected = sorted(self.data)
        retrieved = sorted([(int(k), v) for k,v in self.db.reader().get_all()])

        self.assertListEqual(retrieved, expected)

    # @unittest.skip("skipping checking multiple processes")
    def test_save_in_multiple_processes(self):

        import multiprocessing
        coef = len(self.data) // self.processes_number
        processes = [
            multiprocessing.Process(
                target=save_in_process,
                # args=[self.db.root, self.data[(i*coef):((i+1)*coef)]]
                args=[self.db.root, self.data]
            )
            for i in range(self.processes_number)
        ]
        for process in processes: process.start()
        for process in processes: process.join()

        expected = sorted(self.data)
        retrieved = sorted([(int(k), v) for k,v in self.db.reader().get_all()])

        self.assertListEqual(retrieved, expected)


def save_in_process(root_dir, data_to_save):
    """
    Helper function to run in subprocess.
    """
    db = DB.load(root_dir)
    with db.writer() as writer:
        for k, v in data_to_save:
            writer[k] = v


class ReformatTest(unittest.TestCase):
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



if __name__ == '__main__':
    import logging
    # logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
    unittest.main()
