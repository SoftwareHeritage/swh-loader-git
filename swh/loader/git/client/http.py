#!/usr/bin/env python3

# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import requests

from retrying import retry

from swh.loader.git.retry import policy
from swh.loader.git.storage import storage
from swh.loader.git.protocols import serial


session_swh = requests.Session()


def compute_simple_url(base_url, type):
    """Compute the api url.

    """
    return '%s%s' % (base_url, type)


@retry(retry_on_exception=policy.retry_if_connection_error,
       wrap_exception=True,
       stop_max_attempt_number=3)
def execute(map_type_url,
            method_fn,
            base_url,
            obj_type,
            data,
            result_fn=lambda result: result.ok):
    """Execute a query to the backend.
    - map_type_url is a map of {type: url backend}
    - method_fn is swh_session.post or swh_session.put
    - base_url is the base url of the backend
    - obj_type is the nature of the data
    - data is the data to send to the backend
    - result_fn is a function which takes the response
    result and do something with it. The default function
    is to return if the server is ok or not.

    """
    if not data:
        return data

    res = method_fn(compute_simple_url(base_url, map_type_url[obj_type]),
                    data=serial.dumps(data),
                    headers={'Content-Type': serial.MIMETYPE})
    return result_fn(res)


# url mapping for lookup
url_lookup_per_type = {
    storage.Type.origin: "/origins/",
    storage.Type.content: "/vcs/contents/",
    storage.Type.directory: "/vcs/directories/",
    storage.Type.revision: "/vcs/revisions/",
}


def post(base_url, obj_type, obj_sha1s):
    """Retrieve the objects of type type with sha1 sha1hex.

    """
    return execute(url_lookup_per_type,
                   session_swh.post,
                   base_url,
                   obj_type,
                   obj_sha1s,
                   result_fn=lambda res: serial.loads(res.content))


# url mapping for storage
url_store_per_type = {
    storage.Type.origin: "/origins/",
    storage.Type.content: "/vcs/contents/",
    storage.Type.directory: "/vcs/directories/",
    storage.Type.revision: "/vcs/revisions/",
    storage.Type.release: "/vcs/releases/",
    storage.Type.occurrence: "/vcs/occurrences/",
    storage.Type.person: "/vcs/persons/",
}


def put(base_url, obj_type, obj):
    """Given an obj (map, simple object) of obj_type, PUT it in the backend.

    """
    return execute(url_store_per_type,
                   session_swh.put,
                   base_url,
                   obj_type,
                   obj)
