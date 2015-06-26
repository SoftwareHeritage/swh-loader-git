#!/usr/bin/env python3

# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import requests
import json

from retrying import retry

from swh.storage import models
from swh.retry import policy


_api_url = {models.Type.blob: '/git/blobs/',
            models.Type.commit: '/git/commits/',
            models.Type.tree: '/git/trees/'}

session_swh = requests.Session()


def compute_url(baseurl, type, sha1hex):
    """Compute the api url.
    """
    return '%s%s%s' % (baseurl, _api_url[type], sha1hex)


def compute_simple_url(baseurl, type):
    """Compute the api url.
    """
    return '%s%s' % (baseurl, type)


@retry(retry_on_exception=policy.retry_if_connection_error,
       wrap_exception=True,
       stop_max_attempt_number=3)
def get(baseurl, type, sha1hex):
    """Retrieve the objects of type type with sha1 sha1hex.
    """
    r = session_swh.get(compute_url(baseurl, type, sha1hex))
    return r.ok


@retry(retry_on_exception=policy.retry_if_connection_error,
       wrap_exception=True,
       stop_max_attempt_number=3)
def put(baseurl, type, sha1hex, data=None):
    """Retrieve the objects of type type with sha1 sha1hex.
    """
    r = session_swh.put(compute_url(baseurl, type, sha1hex),
                        [] if data is None else data)
    return r.ok


@retry(retry_on_exception=policy.retry_if_connection_error,
       wrap_exception=True,
       stop_max_attempt_number=3)
def post(baseurl, sha1s):
    """Retrieve the objects of type type with sha1 sha1hex.
    """
    url = compute_simple_url(baseurl, "/objects/")
    r = session_swh.post(url,
                         data=json.dumps(sha1s),
                         headers={'Content-type': 'application/json'})
    result = r.json()
    return result['sha1s']
