# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from requests import ConnectionError


def retry_if_io_error(exc):
    """Return True if IOError,
       False otherwise.
    """
    return isinstance(exc, IOError)


def retry_if_connection_error(exc):
    """Return True if ConnectionError,
       False otherwise.
    """
    return isinstance(exc, ConnectionError)
