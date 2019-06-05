import unittest
import threading

from mystore.errors import BaseUnitDoesNotExist

from tests.helpers import DBTestsSetup


class DBReaderTest(DBTestsSetup, unittest.TestCase):
    def test_get_value(self):
        """ """
        with self.db.reader() as reader:
            retrieved_data = [(k, reader[k]) for k, v in self.data]
        self.assertListEqual(retrieved_data, self.data)

    def test_getitem_nonexisting_file_r(self):
        """ """
        with self.assertRaises(BaseUnitDoesNotExist):
            with self.db.reader("r") as reader:
                retrieved_value = reader[10*10]

    def test_getitem_nonexisting_file_R(self):
        """ """
        with self.assertRaises(BaseUnitDoesNotExist):
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
