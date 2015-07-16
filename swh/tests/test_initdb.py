# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.manager import manage


def prepare_db(db_url):
    """DB fresh start.
    """
    manage('cleandb', db_url)
    manage('initdb', db_url)
