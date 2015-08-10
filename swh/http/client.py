#!/usr/bin/env python3

# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import requests

from retrying import retry

from swh.retry import policy
from swh.storage import store
from swh.protocols import serial

session_swh = requests.Session()


def compute_simple_url(baseurl, type):
    """Compute the api url.
    """
    return '%s%s' % (baseurl, type)


# url mapping
url_lookup_per_type = {store.Type.origin: "/origins/",
                       store.Type.content: "/vcs/contents/",
                       store.Type.directory: "/vcs/directories/",
                       store.Type.revision: "/vcs/revisions/"
                       }


# @retry(retry_on_exception=policy.retry_if_connection_error,
#        wrap_exception=True,
#        stop_max_attempt_number=3)
def post(baseurl, obj_type, obj_sha1s):
    """Retrieve the objects of type type with sha1 sha1hex.
    """
    if not obj_sha1s:
        return []

    url = compute_simple_url(baseurl, url_lookup_per_type[obj_type])
    body = serial.dumps(obj_sha1s)
    r = session_swh.post(url,
                         data=body,
                         headers={'Content-Type': serial.MIMETYPE})
    return serial.loads(r.content)


# @retry(retry_on_exception=policy.retry_if_connection_error,
#        wrap_exception=True,
#        stop_max_attempt_number=3)
def put(baseurl, obj_type, obj):
    """Store the obj of type obj_type in backend.
       Return the identifier held in the key 'key_result' of the server's
       response.
    """
    if not obj:
        return None

    url = compute_simple_url(baseurl, url_store_per_type[obj_type])
    body = serial.dumps(obj)
    r = session_swh.put(url,
                           data=body,
                           headers={'Content-Type': serial.MIMETYPE})
    return serial.loads(r.content)


url_store_per_type = {store.Type.origin: "/origins/",
                      store.Type.content: "/vcs/contents/",
                      store.Type.directory: "/vcs/directories/",
                      store.Type.revision: "/vcs/revisions/",
                      store.Type.release: "/vcs/releases/",
                      store.Type.occurrence: "/vcs/occurrences/",
                     }


# @retry(retry_on_exception=policy.retry_if_connection_error,
#        wrap_exception=True,
#        stop_max_attempt_number=3)
def put_all(baseurl, obj_type, objs_map):
    """Given a list of sha1s, put them in the backend."""
    if not objs_map:
        return []

    url = compute_simple_url(baseurl, url_store_per_type.get(obj_type, "/objects/"))
    body = serial.dumps(objs_map)
    r = session_swh.put(url,
                        data=body,
                        headers={'Content-Type': serial.MIMETYPE})
    return serial.loads(r.content)
