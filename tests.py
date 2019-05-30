"""
Run unittests:
python tests.py -v
"""
import unittest
import os
import sys
import tempfile
import gzip
import json
import time
import shutil
import random

from dbmdb import JsonDbmFile
from dbmdb import Router
from dbmdb import Dbmdb
from dbmdb import Writer
from dbmdb import Reader
from dbmdb.main import METADATA_FILENAME
import dbmdb


def get_dbmdb_path():
    return os.path.join(
        tempfile.gettempdir(),
        "dbmdb_%s_%s" % (time.time(), random.randint(0, 10000))
    )


class DbmFileTest(unittest.TestCase):
    """
    Test JsonDbmFile wrapper class
    """
    @classmethod
    def setUpClass(cls):
        filepath = os.path.join(tempfile.gettempdir(), "temp_dbm.dbm")
        testdata = {
            "0": {"name": "zero"},
            "1": {"name": "one"},
            "10": {"name": "ten"},
            "99": {"name": "ninety nine"},
            "100": None,
        }
        with JsonDbmFile(filepath) as f:
            for k, v in testdata.items():
                f[str(k)] = v
        cls._filepath = filepath
        cls._testdata = testdata

    @classmethod
    def tearDownClass(cls):
        for ext in ["", ".dir", ".dat", ".pag", ".bak"]:
            filepath = cls._filepath + ext
            if os.path.exists(filepath):
                os.remove(filepath)

    # setup and teardown for each test
    def setUp(self):
        self.dbmfile = JsonDbmFile(self._filepath, mode="r")

    def tearDown(self):
        self.dbmfile.close()

    # tests for methods
    def test_str(self):
        name = str(self.dbmfile)
        self.assertEqual(name, self._filepath)

    def test_get(self):
        keys = ["1", "100"]
        testdata_values = [self._testdata[key] for key in keys]
        values = [self.dbmfile.get(key) for key in keys]
        self.assertEqual(values, testdata_values)

    def test_get_raw(self):
        keys = ["1", "100"]
        testdata_values = [
            gzip.compress(json.dumps(self._testdata[key]).encode())[8:]
            # bytes 4-8 are for timestamp:
            # http://www.onicos.com/staff/iz/formats/gzip.html
            for key in keys]
        values = [self.dbmfile.get(key, raw=True)[8:] for key in keys]
        self.assertEqual(values, testdata_values)

    def test_keys(self):
        keys = self.dbmfile.keys()
        self.assertEqual(sorted([k.decode() for k in keys]), sorted(self._testdata.keys()))

    def test_get_all_items(self):
        all_values = {k.decode(): self.dbmfile.get(k) for k in self.dbmfile.keys()}
        self.assertDictEqual(all_values, self._testdata)



class RouterTest(unittest.TestCase):
    """
    Tests for all things create/initialize DB
    """
    def setUp(self):
        self.root_directory = get_dbmdb_path()
        os.makedirs(self.root_directory)

        sep = os.path.sep
        ext = ".dbm"
        self.data = [
            # dbm_size, subfolder_size, key, expected_filename(v1), expected filename(v0)
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
        shutil.rmtree(self.root_directory, ignore_errors=True)

    def test_path_retrieval_fk0(self):
        """
        Test function converting generating path to a dbm file given
        (a) configuration attributes and
        (b) key value
        """
        routers = [Router(self.root_directory, row[0], row[1], first_key=0) for row in self.data]
        expected_values = [os.path.join(self.root_directory, row[3]) for row in self.data]
        returned_values = [router.get_path_to_dbm(row[2]) for router, row in zip(routers, self.data)]
        self.assertListEqual(returned_values, expected_values)

    def test_path_retrieval_fk1(self):
        """
        Test an older version which assumed that keys start from 1
        """
        routers = [Router(self.root_directory, row[0], row[1], first_key=1) for row in self.data]
        expected_values = [os.path.join(self.root_directory, row[4]) for row in self.data]
        returned_values = [router.get_path_to_dbm(row[2]) for router, row in zip(routers, self.data)]
        self.assertListEqual(returned_values, expected_values)



class DbmdbTest(unittest.TestCase):

    def setUp(self):
        self.data = [(i, {"entry_key":i, "value": "some value %s" % str(i)}) for i in range(0,10)]
        self.root_directory = get_dbmdb_path()
        self.dbm_size = 3
        self.subfolder_size = 1
        self.router = Router(self.root_directory, dbm_size=10, subfolder_size=0)
        self.db = Dbmdb(self.router)
        self.db.create()

        writer = self.db.get_writer()
        for k, v in self.data:
            writer.save_value(k, v)
        writer.close()

    def tearDown(self):
        shutil.rmtree(self.root_directory, ignore_errors=True)
        self.db = None

    def test_create(self):
        """
        Test from_folder method.
        """
        # initialize an existing DB from serialized configs
        db = Dbmdb.from_folder(self.root_directory)

        # assert that it has same configuration attributes
        keys_to_compare = ["root_directory", "dbm_size", "subfolder_size", "first_key"]
        params = [[getattr(router, k) for k in keys_to_compare]
                  for router in (self.db.router, db.router)]

        self.assertListEqual(params[0], params[1])

    def test_get(self):
        retrieved_data = []
        for k, v in self.data:
            retrieved_data += [(k, self.db.get(k))]
        self.assertListEqual(retrieved_data, self.data)

    def test_get_many(self):
        keys = [k for k, v in self.data]
        retrieved_data = self.db.get_many(keys=keys)    # dict
        retrieved = sorted(retrieved_data.items())
        expected = sorted(self.data)
        self.assertListEqual(retrieved, expected)

    def test_get_many_in_threads(self):
        keys = [k for k, v in self.data]
        retrieved_data = self.db.get_many(keys=keys, num_threads=2)    # dict
        retrieved = sorted(retrieved_data.items())
        expected = sorted(self.data)
        self.assertListEqual(retrieved, expected)

    def test_all_filepaths(self):
        expected_dbms = [self.db.router.get_path_to_dbm(k) for k, v in self.data]
        expected_dbms = set(expected_dbms)
        retrieved_dbms = set(self.db._all_filepaths())
        self.assertSetEqual(retrieved_dbms, expected_dbms)

    def test_get_all(self):
        retrieved_values = sorted([(k, v) for k,v in self.db.get_all()])
        expected_values = sorted(self.data)
        self.assertListEqual(retrieved_values, expected_values)

    def test_create_dbmdb_nonempty_dir(self):
        """
        Try creating another db in existing non-empty directory.
        """
        with self.assertRaises(dbmdb.DbmdbError):
            db = dbmdb.create_db(self.root_directory, 10, 0, first_key=0)




class ReaderWriterTest(unittest.TestCase):

    def setUp(self):
        self.data = [(i, {"entry_key":i, "value": "some value %s" % str(i)})
                     for i in range(0,10)]
        self.root_directory = get_dbmdb_path()
        self.dbm_size = 3
        self.subfolder_size = 1
        self.router = Router(self.root_directory, self.dbm_size, self.subfolder_size)

        # fill database with some data
        writer = Writer(self.router)
        for int_key, json_value in self.data:
            writer.save_value(key=int_key, value=json_value)
        writer.close()

    def tearDown(self):
        shutil.rmtree(self.root_directory, ignore_errors=True)
        self.router = None

    def test_get_value(self):
        reader = Reader()
        retrieved_data = [(k, reader.get_value(k, self.router.get_path_to_dbm(k))) for k,v in self.data]
        self.assertListEqual(retrieved_data, self.data)

    def test_get_value_concurrent(self):
        retrieved_data = []
        def save_in_thread(root_directory, keys_to_read, lock):
            nonlocal retrieved_data
            reader = Reader(threadlock=lock)
            for k in keys_to_read:
                v = reader.get_value(k, self.router.get_path_to_dbm(k))
                with lock:
                    retrieved_data += [(k, v)]
            reader.close()

        import threading
        threads_number = 5
        lock = threading.Lock()
        coef = len(self.data) // threads_number
        threads = [
            threading.Thread(
                target=save_in_thread,
                args=[
                    self.root_directory,
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


class DbmdbJsonTest(unittest.TestCase):

    def setUp(self):
        self.data = [(i, {"entry_key":i, "value": "some value %s" % str(i)})
                     for i in range(0,10)]
        self.root_directory = get_dbmdb_path()
        self.dbm_size = 3
        self.subfolder_size = 1
        self.router = Router(self.root_directory, self.dbm_size, self.subfolder_size)
        self.db = Dbmdb(self.router)
        self.db.create()

        self.root_directory2 = get_dbmdb_path() + "2"
        self.root_directory3 = get_dbmdb_path() + "3"

        writer = self.db.get_writer()
        for k, v in self.data:
            writer.save_value(k, v)
        writer.close()

    def tearDown(self):
        shutil.rmtree(self.root_directory, ignore_errors=True)
        shutil.rmtree(self.root_directory2, ignore_errors=True)
        shutil.rmtree(self.root_directory3, ignore_errors=True)
        self.db = None

    def test_reformat_as_json(self):
        self.db.reformat_as_json(new_root=self.root_directory2)
        expected_dbms = [os.path.relpath(self.db.router.get_path_to_dbm(k), self.root_directory)
                         for k, v in self.data]
        expected_dbms = set(expected_dbms)
        new_filepaths = set()
        for dirpath, dirnames, filenames in os.walk(self.root_directory2):
            for fn in filenames:
                if fn != METADATA_FILENAME:
                    new_filepaths.add(
                        os.path.relpath(os.path.join(dirpath, fn), self.root_directory2))
        self.assertSetEqual(expected_dbms, new_filepaths)

    def test_reformat_from_json(self):
        self.db.reformat_as_json(new_root=self.root_directory2)
        json_db = Dbmdb.from_folder(self.root_directory2)
        json_db.reformat_from_json(new_root=self.root_directory3)
        new_db = Dbmdb.from_folder(self.root_directory3)
        old_values = sorted([(k, v) for k,v in self.db.get_all()])
        new_values = sorted([(k, v) for k,v in new_db.get_all()])
        self.assertListEqual(old_values, new_values)

    def test_reformat_from_json_raw(self):
        self.db.reformat_as_json(new_root=self.root_directory2, raw=True)
        json_db = Dbmdb.from_folder(self.root_directory2)
        json_db.reformat_from_json(new_root=self.root_directory3, raw=True)
        new_db = Dbmdb.from_folder(self.root_directory3)
        old_values = sorted([(k, v) for k,v in self.db.get_all()])
        new_values = sorted([(k, v) for k,v in new_db.get_all()])
        self.assertListEqual(old_values, new_values)


class DbmdbConcurrencyTest(unittest.TestCase):
    """
    Tests to check correctness of concurrent saving from different instances.
    """
    def setUp(self):
        self.data = [(i, {"entry_key":i, "value": "some value %s" % str(i)})
                     for i in range(0,10)]
        self.threads_number = 5
        self.processes_number = 5
        self.db = dbmdb.create_db(root_directory=get_dbmdb_path(), dbm_size=3, subfolder_size=1)

    def tearDown(self):
        shutil.rmtree(self.db.router.root_directory, ignore_errors=True)
        self.db = None

    #@unittest.skip("skipping checking threads")
    def test_threaded_save(self):
        def save_in_thread(root_directory, data_to_save, lock):
            """ helper function to run in thread """
            mydb = dbmdb.get_db(root_directory)
            writer = mydb.get_writer(threadlock=lock)
            for k, v in data_to_save:
                writer.save_value(k, v)
            writer.close()

        import threading
        lock = threading.Lock()
        coef = len(self.data) // self.threads_number
        threads = [
            threading.Thread(
                target=save_in_thread,
                args=[self.db.router.root_directory, self.data[(i*coef):((i+1)*coef)],lock]
            )
            for i in range(self.threads_number)
        ]
        for thread in threads: thread.start()
        for thread in threads: thread.join()

        expected = sorted(self.data)
        retrieved = sorted([(k, v) for k,v in self.db.get_all()])

        self.assertListEqual(retrieved, expected)

    #@unittest.skip("skipping checking multiple processes")
    def test_multiprocessing_save(self):

        import multiprocessing
        coef = len(self.data) // self.processes_number
        processes = [
            multiprocessing.Process(
                target=save_in_process,
                args=[self.db.router.root_directory, self.data[(i*coef):((i+1)*coef)]]
            )
            for i in range(self.processes_number)
        ]
        for process in processes: process.start()
        for process in processes: process.join()

        expected = sorted(self.data)
        retrieved = sorted([(k, v) for k,v in self.db.get_all()])

        self.assertListEqual(retrieved, expected)


def save_in_process(root_directory, data_to_save):
    """
    Helper function to run in subprocess.
    """
    mydb = Dbmdb.from_folder(root_directory)
    writer = mydb.get_writer()
    for k, v in data_to_save:
        writer.save_value(k, v)
    writer.close()


if __name__ == '__main__':
    unittest.main()
