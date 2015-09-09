# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

from swh.loader.git import git, remote_store, local_store


_load_to_back_fn = {'remote': remote_store.load_to_back
                   ,'local': local_store.prepare_and_load_to_back
                   }


def check_user_conf(conf):
    """Check the user's configuration and rejects if problems.
    """
    action = conf['action']
    if action != 'load':
        return 'skip unknown action %s' % action

    backend_type = conf['backend-type']
    if backend_type not in _load_to_back_fn:
        return 'skip unknown backend-type %s (only `remote`, `local` supported)' % backend_type

    repo_path = conf['repo_path']
    if not os.path.exists(repo_path):
        return 'Repository %s does not exist.' % repo_path

    return None


def load(conf):
    """According to action, load the repo_path.

    used configuration keys:
    - action: requested action
    - repo_path: git repository path ('load' action only)
    - backend-type: backend access's type (remote or local)
    - backend: url access to backend api
    """
    error_msg = check_user_conf(conf)
    if error_msg:
        logging.error(error_msg)
        raise Exception(error_msg)

    repo_path = conf['repo_path']
    logging.info('load repo_path %s' % repo_path)

    load_to_back_fn = _load_to_back_fn[conf['backend-type']]
    backend_setup = conf['backend']

    for swh_repo_snapshot in git.parse(repo_path):
        load_to_back_fn(backend_setup, swh_repo_snapshot)
