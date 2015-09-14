# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time
from datetime import timedelta, datetime, tzinfo


class FixedOffset(tzinfo):
    """Fixed offset in minutes east from UTC.

    """
    def __init__(self, offset, name):
        self.__offset = timedelta(minutes = offset)
        self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return timedelta(0)


_cache_tz = {}


def ts_to_str(timestamp, offset):
    """Convert a timestamp to string.

    """
    if offset in _cache_tz:
        _tz = _cache_tz[offset]
    else:
        _tz = FixedOffset(offset, 'swh')
        _cache_tz[offset] = _tz

    dt = datetime.fromtimestamp(timestamp, _tz)
    return str(dt)


def now():
    """Build the date as of now in the api's format.

    """
    return time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())
