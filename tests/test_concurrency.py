import unittest
import shutil
import threading
import multiprocessing

from mystore import DB

from tests.helpers import get_db_path


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
        self.db = DB(self.root_dir, self.params).create()
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
