"""
To run unittests:
python -m unittest tests -v
(only runs tests imported in __init__.py)
or
python -m unittest discover tests
(will discover tests even if not imported  in __init__.py)

To run individual modules:
python -m unittest tests.test_routers -v

Also see:
python -m unittest -h (help + run examples)
"""
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')

from .test_routers import OriginalRouterTest
from .test_basefiles import DbmFileTest
from .test_db_create import DBCreateTest
from .test_db_io import DBReaderTest
from .test_concurrency import DBConcurrencyTest
from .test_db_reformat import DBReformatTest
