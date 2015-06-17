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
    'file_content_storage_dir':  'swh-git-loader/file-content-storage',
    'object_content_storage_dir':  'swh-git-loader/object-content-storage',
    'folder_depth':  4,
    'blob_compression': None,
}

conf['action'] = 'cleandb'
loader.load(conf)

conf['action'] = 'initdb'
loader.load(conf)

conf['action'] = 'load'
loader.load(conf)
