# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time
import os
import shutil
import tempfile

from swh.backend import api

import test_initdb

def now():
    """Build the date as of now in the api's format.

    """
    return time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())


def list_files_from(root_path):
    """Compute the list of files from root_path.

    """
    f = []
    for (dirpath, dirnames, filenames) in os.walk(root_path):
        f.extend(filenames)
    return f


def app_client(db_url="dbname=softwareheritage-dev-test"):
    """Setup the application ready for testing.

    """
    content_storage_dir = tempfile.mkdtemp(prefix='test-swh-git-loader.',
                     dir='/tmp')
    api.app.config['conf'] = {'db_url': db_url,
                              'content_storage_dir': content_storage_dir,
                              'log_dir': '/tmp/swh-git-loader/log',
                              'folder_depth': 2,
                              'storage_compression': None,
                              'debug': 'true'}

    api.app.config['TESTING'] = True
    app = api.app.test_client()
    test_initdb.prepare_db(db_url)
    return app, db_url, content_storage_dir


def app_client_teardown(content_storage_dir):
    """Tear down app client's context.

    """
    shutil.rmtree(content_storage_dir, ignore_errors=True)
