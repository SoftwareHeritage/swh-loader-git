#!/usr/bin/env python3

# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh import db
from swh.gitloader import models

def manage(conf):
    """According to action, load the repository.

    used configuration keys:
    - action: requested action [cleandb|initdb]
    """
    with db.connect(conf['db_url']) as db_conn:
        action = conf['action']

        if action == 'cleandb':
            logging.info('clean database')
            models.cleandb(db_conn)
        elif action == 'initdb':
            logging.info('initialize database')
            models.initdb(db_conn)
        else:
            logging.warn('skip unknown-action %s' % action)