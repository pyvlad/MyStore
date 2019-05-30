"""
This module contains the Dbmdb class,
main class of the package which represents a database.
"""
import logging
lg = logging.getLogger(__name__)

import os
import dbm
import json
import base64
import shutil
import collections
import threading
import queue

from .dbmfile import JsonDbmFile
from .router import Router
from .reader import Reader
from .writer import Writer
from .misc import DbmdbError


METADATA_FILENAME = ".dbmdb.json"


class Dbmdb:
    """
    Simple database where data is stored by integer key in a tree of dbm files.
    """
    def __init__(self, router):
        self.router = router

    def create(self):
        """
        Create new Dbmdb at self.router.root_directory.
        The folder must be empty or not exist.
        """
        if not os.path.exists(self.router.root_directory):
            os.makedirs(self.router.root_directory)
        elif os.listdir(self.router.root_directory):
            raise DbmdbError("Root directory for database is not empty")
        meta = json.dumps({
            "version": 0 if self.router.first_key == 1 else 1,
            "dbm_size": self.router.dbm_size,
            "subfolder_size": self.router.subfolder_size,
        })
        meta_filepath = os.path.join(self.router.root_directory, METADATA_FILENAME)
        with open(meta_filepath, "w", encoding="utf8") as f:
            f.write(meta)

    @classmethod
    def from_folder(cls, folder):
        metadata_filepath = os.path.join(folder, METADATA_FILENAME)
        with open(metadata_filepath, encoding="utf8") as f:
            data = json.load(f)
        router = Router(
            folder,
            data["dbm_size"],
            data["subfolder_size"],
            first_key=1 if data["version"] == 0 else 0
        )
        return cls(router)


    def get_writer(self, threadlock=None):
        return Writer(self.router, threadlock)


    def get_value(self, key, raw=False):
        """
        Arguments
        ---------
        key: int
            Integer key.
        value: bool
            Pass True to retrieve raw compressed JSON.
        """
        reader = Reader()
        value = reader.get_value(key, self.router.get_path_to_dbm(key), raw)
        reader.close()
        return value

    def get(self, key, raw=False):
        """
        Same as 'get'.
        """
        return self.get_value(key, raw)


    def get_many(self, keys, raw=False, num_threads=0):
        """
        Get many values at once (to minimize dbm open/close calls) and return as dict.
        Key order is not preserved.
        """
        keys_and_paths = [(key, self.router.get_path_to_dbm(key)) for key in keys]
        data = {}

        if not num_threads:
            keys_and_paths = sorted(keys_and_paths, key=lambda x:x[1])
            reader = Reader()
            for key, path in keys_and_paths:
                data[key] = reader.get_value(key, path, raw)
            reader.close()

        else:
            keys_by_path = collections.defaultdict(list)
            for k, path in keys_and_paths:
                keys_by_path[path] += [k]

            queue_in = queue.Queue()
            queue_out = queue.Queue()

            def read(queue_in, queue_out):
                reader = Reader()
                while True:
                    task = queue_in.get()
                    if task is None:
                        queue_out.put(None)
                        break
                    filepath, keys = task
                    keys_dict = {k: reader.get_value(k, filepath, raw) for k in keys}
                    queue_out.put(keys_dict)
                reader.close()

            workers = [
                threading.Thread(
                    target=read,
                    args=(queue_in, queue_out)
                ) for i in range(num_threads)
            ]
            for thread in workers:
                thread.start()

            for path, keys in keys_by_path.items():
                queue_in.put((path, keys))
            for i in range(num_threads):
                queue_in.put(None)

            finished = 0
            while True:
                objs_by_key = queue_out.get()
                if objs_by_key is None:
                    finished += 1
                else:
                    data.update(objs_by_key)
                if finished == num_threads:
                    break

        return data

    def get_all(self, raw=False):
        """
        [Generator]
        Return all values:
        go through all files in Dbmdb and return all key, value pairs from each file.
        """
        for filepath in self._all_filepaths():
            dbmfile = JsonDbmFile(path=filepath, mode="r")
            content = {k.decode(): dbmfile.get(k, raw=raw) for k in dbmfile.keys()}
            for k,v in content.items():
                yield int(k), v
            dbmfile.close()

    def _all_filepaths(self):
        for dirpath, dirnames, filenames in os.walk(self.router.root_directory):
            dbms_in_folder = set()
            for fn in filenames:
                path = os.path.join(dirpath, fn)
                if path[-4:] in [".dir", ".pag", ".dat", ".bat"]:
                    path = path[:-4]
                if dbm.whichdb(path):
                    dbms_in_folder.add(path)
            for dbmpath in dbms_in_folder:
                lg.debug("yielding dbmpath: %s" % dbmpath)
                yield dbmpath

    def reformat_as_json(self, new_root, raw=False):
        """
        Create a copy of a DbmDb with JSON files instead of dbm files.
        """
        if not os.path.exists(new_root):
            os.makedirs(new_root)
        dbmdb_metafile = os.path.join(self.router.root_directory, METADATA_FILENAME)
        new_metafile = os.path.join(new_root, METADATA_FILENAME)
        shutil.copy2(dbmdb_metafile, new_metafile)

        for filepath in self._all_filepaths():
            # get new filename and create new subdirectory if necessary
            filerelpath = os.path.relpath(filepath, self.router.root_directory)
            filepath_new = os.path.join(new_root, filerelpath)
            dirpath_new = os.path.dirname(filepath_new)
            if not os.path.exists(dirpath_new):
                os.makedirs(dirpath_new)

            # read old file content
            dbmfile = JsonDbmFile(path=filepath, mode="r")
            content = {int(k.decode()): dbmfile.get(k, raw=raw) for k in dbmfile.keys()}
            dbmfile.close()

            # reformat content and write new file
            new_content = content if not raw else {k: base64.encodebytes(v).decode('ascii')
                                                   for k,v in content.items()}
            file_content = json.dumps(new_content)
            with open(filepath_new, "w") as f:
                f.write(file_content)


    def reformat_from_json(self, new_root, raw=False):
        """
        Create a copy of a DbmDb with dbm files instead of JSON files.
        """
        new_router = Router(
            new_root,
            self.router.dbm_size,
            self.router.subfolder_size,
            self.router.first_key
        )
        new_db = Dbmdb(new_router)
        new_db.create()

        dbmdb_metafile = os.path.join(self.router.root_directory, METADATA_FILENAME)
        writer = new_db.get_writer()

        for dirpath, dirnames, filenames in os.walk(self.router.root_directory):
            jsons_in_folder = set()
            for fn in filenames:
                path = os.path.join(dirpath, fn)
                if path != dbmdb_metafile:
                    jsons_in_folder.add(path)
            for jsonpath in jsons_in_folder:
                lg.debug("next jsonpath: %s" % jsonpath)
                with open(jsonpath, "r") as f:
                    file_content = json.loads(f.read())
                if raw:
                    content = {int(k): base64.decodebytes(v.encode('ascii'))
                               for k,v in file_content.items()}
                else:
                    content = {int(k): v for k,v in file_content.items()}
                # reformat content and write new file
                for k,v in content.items():
                    writer.save_value(k,v)

        writer.close()

    def reformat_as_files(self, new_root):
        """
        Create a copy of a DbmDb with JSON files instead of dbm files.
        """
        if not os.path.exists(new_root):
            os.makedirs(new_root)

        for dbmpath in self._all_filepaths():
            with JsonDbmFile(path=dbmpath, mode="r") as jdf:
                content = {int(k.decode()): jdf.get(k, raw=True) for k in jdf.keys()}
                for k, v in content.items():
                    str_key = "%07d" % k
                    dirname = os.path.join(new_root, str_key[:-5], str_key[-5:-3])
                    filename = str_key[-3:]
                    filepath = os.path.join(dirname, filename)
                    if not os.path.exists(dirname):
                        try:
                            os.makedirs(dirname)
                        except FileExistsError:
                            pass
                    with open(filepath, "wb") as f:
                        f.write(v)
            lg.info(dbmpath)
