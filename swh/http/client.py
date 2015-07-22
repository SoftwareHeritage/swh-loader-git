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


session_swh = requests.Session()


def compute_simple_url(baseurl, type):
    """Compute the api url.
    """
    return '%s%s' % (baseurl, type)


def to_unicode(s):
    """Convert s to unicode.
    FIXME: Technically, there is a serialization problem.
    Wrap bytes into string (unicode in python3) to serialize in json.
    """
    return str(s) if isinstance(s, bytes) else s


def serialize_object(sha1hex, obj):
    """Given a sha1hex and an swh object, build query data structure for backend.
    """
    obj_type = obj.type()
    raw_data = to_unicode(obj.read_raw())
    type_value = obj_type.value
    if obj_type == models.Type.blob:
        return {'type': type_value,
                'content': raw_data,
                'sha1': sha1hex,
                'size': obj.size(),
                'git-sha1': obj.sha1()}
    else:
        return {'type': type_value,
                'sha1': sha1hex,
                'content': raw_data}


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


@retry(retry_on_exception=policy.retry_if_connection_error,
       wrap_exception=True,
       stop_max_attempt_number=3)
def put_all(baseurl, sha1s_hex, sha1s_map):
    """Given a list of sha1s, put them in the backend."""
    json_payload = {}
    for sha1_hex in sha1s_hex:
        obj = sha1s_map.get_obj(sha1_hex)
        json_payload[sha1_hex] = serialize_object(sha1_hex, obj)

    url = compute_simple_url(baseurl, "/objects/")
    session_swh.put(url,
                    data=json.dumps(json_payload),
                    headers={'Content-type': 'application/json'})
