# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import pygit2
import hashlib

from sgloader.models import find_file, find_object, add_file, add_object


def load_repo(parent_repo_path):
    """Load the repository path.
    """
    repo_path = pygit2.discover_repository(parent_repo_path)

    return pygit2.Repository(repo_path)


def commits_from(repo, commit):
    """Return the lists of commits from a given commit.
    """
    return repo.walk(commit.id, pygit2.GIT_SORT_TIME)


def _hashkey(data):
    """Given some data, compute the sha256 of such data.
    """
    sha256 = hashlib.sha256()
    sha256.update(data)
    return sha256.hexdigest()


def in_cache_objects(db_conn, obj):
    """Determine if a commit is in the cache.
    """
    return find_object(db_conn, obj.hex) is not None


def add_object_in_cache(db_conn, obj, obj_type):
    """Add obj in cache.
    """
    if in_cache_objects(db_conn, obj):
        logging.debug('Object \'%s\' already present... skip' % obj.hex)
        return

    logging.debug('Injecting object \'%s\' in cache' % obj.hex)

    add_object(db_conn, obj.hex, obj_type)


def in_cache_files(db_conn, blob, hashkey=None):
    """Determine if a file is in the file cache.
    """
    hashkey = _hashkey(blob.data) if hashkey is None else hashkey
    return find_file(db_conn, hashkey) is not None


def add_file_in_cache(db_conn, blob, filepath, status_write):
    """Add file in cache.
    """
    hashkey = _hashkey(blob.data)

    if in_cache_files(db_conn, blob, hashkey):
        logging.debug('Blob \'%s\' already present. skip' % blob.hex)
        return

    logging.debug('Injecting file \'%s\' with \'%s\' (sha256: \'%s\')',
                  blob.hex,
                  filepath,
                  hashkey)

    add_file(db_conn, hashkey, filepath, status_write)


def write_blob_on_disk(blob, filepath):
    """Write blob on disk.
    """
    status_write = True
    data = blob.data

    try:
        data_to_store = data.decode("utf-8")
    except UnicodeDecodeError:
        logging.warn("Problem during decoding blob %s's data... Skip!" %
                     blob.hex)
        # logging.warn("blob's data: '%s'", blob.data)
        # sometimes if fails with cryptic error `UnicodeDecodeError: 'utf-8'
        # codec can't decode byte 0x9d in position 10: invalid start byte`...
        status_write = False  # reference there was some issue
        data_to_store = str(data)

    f = open(filepath, 'w')
    f.write(data_to_store)
    f.close()

    return status_write


def create_dir_from_hash(dataset_dir, hash):
    """Create directory from a given hash.
    """
    def _compute_folder_name(dataset_dir):
        """Compute the folder prefix from a hash key.
        """
        # FIXME: find some split function
        return os.path.join(dataset_dir,
                            hash[0:2],
                            hash[2:4],
                            hash[4:6],
                            hash[6:8])

    folder_in_dataset = _compute_folder_name(dataset_dir)

    try:
        os.makedirs(folder_in_dataset)
    except OSError:
        True  # do nothing

    return folder_in_dataset


def add_file_in_dataset(db_conn, dataset_dir, blob):
    """Add file in the dataset (on disk).

TODO: split in another module, file manipulation maybe?
    """
    hashkey = _hashkey(blob.data)
    folder_in_dataset = create_dir_from_hash(dataset_dir, hashkey)

    filepath = os.path.join(folder_in_dataset, hashkey)
    logging.debug("Injecting file '%s' in dataset." % filepath)

    status_write = write_blob_on_disk(blob, filepath)

    return (filepath, status_write)
