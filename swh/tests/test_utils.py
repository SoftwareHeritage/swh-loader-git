# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time


from swh.backend import api

import test_initdb

def now():
    "Build the date as of now in the api's format."
    return time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())


def app_client(db_url="dbname=softwareheritage-dev-test"):
    """Setup the application ready for testing.
    """
    api.app.config['conf'] = {'db_url': db_url,
                               'content_storage_dir': '/tmp/swh-git-loader/content-storage',
                               'log_dir': '/tmp/swh-git-loader/log',
                               'folder_depth': 2,
                               'storage_compression': None,
                               'debug': 'true'}

    api.app.config['TESTING'] = True
    app = api.app.test_client()
    test_initdb.prepare_db(db_url)
    return app, db_url
