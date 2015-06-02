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

from pprint import pformat
from sqlalchemy import func

from sgloader.db_utils import session_scope
from sgloader.models import Repository


def lookup_repo(db_session, repo_id):
    return db_session.query(Repository) \
                     .filter(Repository.id == repo_id) \
                     .first()


def last_repo_id(db_session):
    t = db_session.query(func.max(Repository.id)) \
                  .first()
    if t is not None:
        return t[0]
    # else: return None


INJECT_KEYS = ['id', 'name', 'full_name', 'html_url', 'description', 'fork']


def inject_repo(db_session, repo):
    logging.debug('injecting repo %d' % repo['id'])
    if lookup_repo(db_session, repo['id']):
        logging.info('not injecting already present repo %d' % repo['id'])
        return
    kwargs = {k: repo[k] for k in INJECT_KEYS if k in repo}
    sql_repo = Repository(**kwargs)
    db_session.add(sql_repo)


def load_repo(parent_repo_path):
    """Load the repository path.
    """
    repo_path = pygit2.discover_repository(parent_repo_path)

    return pygit2.Repository(repo_path) # return the repo's python representation


def commits_from(repo, commit):
    """Return the lists of commits from a given commit."""
    return repo.walk(commit.id, pygit2.GIT_SORT_TIME)


def in_cache_commits(commit):
    """ Determine if a commit is in the cache."""
    return False;
