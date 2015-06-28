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
from swh.gitloader.type import get_obj, get_type


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


def data_object(sha1hex, obj):
    """Build data structure to query the backend.
    """
    type = get_type(obj)
    raw_obj = get_obj(obj)

    data = str(raw_obj.read_raw())
    # raw_data = raw_obj.read_raw()
    # data = raw_data.decode('utf-8') if isinstance(raw_data, bytes) else raw_data

    if type == models.Type.blob:
        data = {'type': type.value,
                'content': data,
                'sha1': sha1hex,
                'size': raw_obj.size,
                'git-sha1': raw_obj.hex}
    else:
        data = {'type': type.value,
                'sha1': sha1hex,
                'content': data}

    return data

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

# @retry(retry_on_exception=policy.retry_if_connection_error,
#        wrap_exception=True,
#        stop_max_attempt_number=3)
def put_all(baseurl, sha1s_hex, sha1s_map):
    """Given a list of sha1s, put them in the backend."""
    json_payload = {}
    for sha1_hex in sha1s_hex:
        obj = sha1s_map[sha1_hex]
        data = data_object(sha1_hex, obj)
        json_payload[sha1_hex] = data

    url = compute_simple_url(baseurl, "/objects/")
    session_swh.put(url,
                    data=json.dumps(json_payload),
                    headers={'Content-type': 'application/json'})
