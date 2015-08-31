# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.manager import manage


def prepare_db(db_url):
    """DB fresh start.
    """
    manage('cleandb', db_url)
    manage('initdb', db_url)
