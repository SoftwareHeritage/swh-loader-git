# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>, Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import gzip
import logging
import os
import re
import requests
import time
import pygit2
import hashlib

from pprint import pformat
from sqlalchemy import func

from sgloader.db_utils import session_scope
from sgloader.models import CommitCache, FileCache


def load_repo(parent_repo_path):
    """Load the repository path.
    """
    repo_path = pygit2.discover_repository(parent_repo_path)

    return pygit2.Repository(repo_path) # return the repo's python representation


def commits_from(repo, commit):
    """Return the lists of commits from a given commit.
    """
    return repo.walk(commit.id, pygit2.GIT_SORT_TIME)


def hashkey(data):
    """Given some data, compute the sha256 of such data.
    """
    sha256 = hashlib.sha256();
    sha256.update(data)
    return str(sha256.digest())
    

def in_cache_files(db_session, blob, key = None):
    """Determine if a file is in the file cache.
    """
    key = hashkey(blob.data) if key is None else key
    return db_session.query(CommitCache) \
                     .filter(FileCache.sha256 == key) \
                     .first()


def add_file_in_cache(db_session, blob, filepath):
    """Add file in cache.
    """
    key = hashkey(blob.data)
    logging.debug('injecting file \'%s\' with \'%s\' (sha256: \'%s\')' % blob.hex % filepath % sha256)
    
    if in_cache_files(db_session, blob, key):
        logging.info('not injecting already present blob \'%d\'' % blob.hex)
        return
    
    kwargs = {'sha256': key, 'path': filepath}
    sql_repo = FileCache(**kwargs)
    db_session.add(sql_repo)


def add_file_in_dataset(db_session, dataset_dir, blob, filepath):
    """Add file in the dataset on disk.
    """
    print ("Add the file to the dataset")
    return False


def in_cache_commits(db_session, commit):
    """Determine if a commit is in the cache.
    """
    return db_session.query(CommitCache) \
                     .filter(CommitCache.sha1 == commit.hex) \
                     .first()


def add_commit_in_cache(db_session, commit):
    """Add commit in cache.
    """
    logging.debug('injecting commit \'%s\'' % commit.hex)
    if in_cache_commits(db_session, commit):
        logging.info('not injecting already present commit \'%d\'' % commit.hex)
        return
    
    kwargs = {'sha1': commit.hex}
    sql_repo = CommitCache(**kwargs)
    db_session.add(sql_repo)
