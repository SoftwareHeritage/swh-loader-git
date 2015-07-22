#!/usr/bin/env python3

# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from swh.gitloader import loader


conf = {
    'db_url': 'dbname=swhgitloader',
    # 'repository': os.path.expanduser('./test-repo'),
    'repository': os.path.expanduser('../debsources'),
    'content_storage_dir':  '/tmp/swh-git-loader/content-storage',
    'folder_depth':  4,
    'storage_compression': None,
}

conf['action'] = 'cleandb'
loader.load(conf)

conf['action'] = 'initdb'
loader.load(conf)

conf['action'] = 'load'
loader.load(conf)
