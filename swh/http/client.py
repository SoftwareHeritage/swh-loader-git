#!/usr/bin/env python3

# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import requests

from swh.storage import models


_api_url = {models.Type.blob: '/git/blobs/',
            models.Type.commit: '/git/commits/',
            models.Type.tree: '/git/trees/'}


def compute_url(baseurl, type, sha1hex):
    """Compute the api url.
    """
    return '%s%s%s' % (baseurl, _api_url[type], sha1hex)


def get(baseurl, type, sha1hex):
    """Retrieve the objects of type type with sha1 sha1hex.
    """
    r = requests.get(compute_url(baseurl, type, sha1hex))
    return r.ok


def put(baseurl, type, sha1hex, data=None):
    """Retrieve the objects of type type with sha1 sha1hex.
    """
    r = requests.put(compute_url(baseurl, type, sha1hex),
                     [] if data is None else data)
    return r.ok
