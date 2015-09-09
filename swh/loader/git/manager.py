#!/usr/bin/env python3

# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.loader.git.store import db, models


def manage(action, db_url):
    """According to action, load the repository.

    used configuration keys:
    - action: requested action [cleandb|initdb]
    """
    with db.connect(db_url) as db_conn:
        if action == 'cleandb':
            logging.info('clean database')
            models.cleandb(db_conn)
        elif action == 'initdb':
            logging.info('initialize database')
            models.initdb(db_conn)
        else:
            logging.warn('skip unknown-action %s' % action)
