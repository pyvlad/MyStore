import unittest
import os
import shutil

from mystore import OriginalRouter

from tests.helpers import get_db_path


class OriginalRouterTest(unittest.TestCase):
    """
    Test whether the OriginalRouter maps keys to paths as expected.
    """
    def setUp(self):
        self.root_dir = get_db_path()
        os.makedirs(self.root_dir)

        sep = os.path.sep
        ext = ".dbm"
        self.ext = ext
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
        routers = [OriginalRouter(self.root_dir, params(row), self.ext) for row in self.data]
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
        routers = [OriginalRouter(self.root_dir, params(row), self.ext) for row in self.data]
        expected_values = [os.path.join(self.root_dir, row[4]) for row in self.data]
        returned_values = [router.get_path(row[2]) for router, row in zip(routers, self.data)]
        self.assertListEqual(returned_values, expected_values)
