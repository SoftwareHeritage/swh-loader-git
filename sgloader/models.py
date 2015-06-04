# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>, Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime

from sqlalchemy import Column
from sqlalchemy import DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base


SQLBase = declarative_base()


class FileCache(SQLBase):

    """File cache"""

    __tablename__ = 'file_cache'

    sha256    = Column(String(65), primary_key = True)
    path      = Column(String    , nullable    = False)
    last_seen = Column(DateTime  , nullable    = False)

    def __init__(self, sha256, path):
        self.sha256    = sha256
        self.path      = path
        self.last_seen = datetime.now()


class ObjectCache(SQLBase):
    """Object cache"""
    types = {"Tag": 3, "Blob": 2, "Tree": 1, "Commit": 0} # FIXME: find pythonic way to do it

    __tablename__ = 'object_cache'

    sha1      = Column(String(41), primary_key = True)
    type      = Column(Integer, nullable       = False)
    last_seen = Column(DateTime, nullable      = False)

    def __init__(self, sha1, type):
        self.sha1      = sha1
        self.type      = type
        self.last_seen = datetime.now()

# FIXME: possible type proposal:
# 0 -> Commit
# 1 -> Tree
# 2 -> Blob
# 3 -> Tag

