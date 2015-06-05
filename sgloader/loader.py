# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import pygit2
import hashlib

from sgloader.models import ObjectCache, FileCache


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


def in_cache_objects(db_session, obj):
    """Determine if a commit is in the cache.
    """
    return db_session.query(ObjectCache) \
                     .filter(ObjectCache.sha1 == obj.hex) \
                     .first()


def add_object_in_cache(db_session, obj, obj_type):
    """Add obj in cache.
    """
    if in_cache_objects(db_session, obj):
        logging.info('Object \'%s\' already present... skip' % obj.hex)
        return

    logging.debug('Injecting object \'%s\' in cache' % obj.hex)

    kwargs = {'sha1': obj.hex, 'type': obj_type}
    sql_repo = ObjectCache(**kwargs)
    db_session.add(sql_repo)
    db_session.commit()


def in_cache_files(db_session, blob, hashkey=None):
    """Determine if a file is in the file cache.
    """
    hashkey = _hashkey(blob.data) if hashkey is None else hashkey
    return db_session.query(FileCache) \
                     .filter(FileCache.sha256 == hashkey) \
                     .first()


def add_file_in_cache(db_session, blob, filepath):
    """Add file in cache.
    """
    hashkey = _hashkey(blob.data)

    if in_cache_files(db_session, blob, hashkey):
        logging.info('Blob \'%s\' already present. skip' % blob.hex)
        return

    logging.debug('Injecting file \'%s\' with \'%s\' (sha256: \'%s\')',
                  blob.hex,
                  filepath,
                  hashkey)

    kwargs = {'sha256': hashkey, 'path': filepath}
    sql_repo = FileCache(**kwargs)
    db_session.add(sql_repo)
    db_session.commit()


def _compute_folder(dataset_dir, hashkey):
    """Compute the folder prefix from a hash key.
    """
    # FIXME: find some split function
    return (dataset_dir,
            hashkey[0:2],
            hashkey[2:4],
            hashkey[4:6],
            hashkey[6:8])


def write_blob_on_disk(blob, filepath):
    """Write blob on disk.
    """
    data = blob.data

    try:
        data_to_store = data.decode("utf-8")
    except UnicodeDecodeError:
        # sometimes if fails with cryptic error `UnicodeDecodeError: 'utf-8'
        # codec can't decode byte 0x9d in position 10: invalid start byte`...
        # what to do?
        logging.warn("Problem during conversion... Skip!")  # Fixme: wtf?
        return

    f = open(filepath, 'w')
    f.write(data_to_store)
    f.close()


def add_file_in_dataset(db_session, dataset_dir, blob):
    """Add file in the dataset (on disk).

TODO: split in another module, file manipulation maybe?
    """
    hashkey = _hashkey(blob.data)
    folder_list = _compute_folder(dataset_dir, hashkey)
    folder_in_dataset = "/".join(folder_list)

    try:
        os.makedirs(folder_in_dataset)
    except OSError:
        logging.warn("Skipping creation of '%s' because it exists already." %
                     folder_in_dataset)

    filepath = os.path.join(folder_in_dataset, hashkey)
    logging.debug("Injecting file '%s' with hash in dataset." % filepath)

    write_blob_on_disk(blob, filepath)

    return filepath
