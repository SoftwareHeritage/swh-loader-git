# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

from swh.gitloader import git, store


def load(conf):
    """According to action, load the repository.

    used configuration keys:
    - action: requested action
    - repository: git repository path ('load' action only)
    - backend_url: url access to backend api
   """
    action = conf['action']

    if action == 'load':
        repo_path = conf['repository']
        backend_url = conf['backend_url']
        logging.info('load repository %s' % repo_path)

        if not os.path.exists(repo_path):
            error_msg = 'Repository %s does not exist.' % repo_path
            logging.error(error_msg)
            raise Exception(error_msg)

        swhrepo = git.parse(repo_path)
        store.load_to_back(backend_url, swhrepo)
    else:
        logging.warn('skip unknown-action %s' % action)
