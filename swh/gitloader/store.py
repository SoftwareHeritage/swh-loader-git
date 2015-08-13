# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage import store
from swh.http import client


def store_objects(backend_url, obj_type, swhmap):
    """Load objects to the backend.
    """
    sha1s = swhmap.keys()
    # have: filter unknown obj
    unknown_obj_sha1s = client.post(backend_url,
                                    obj_type,
                                    sha1s)

    if unknown_obj_sha1s:
        # seen: now create the data for the backend to store
        obj_map = swhmap.objects()
        # store unknown objects
        return client.put(backend_url, obj_type, map(obj_map.get, unknown_obj_sha1s))

    return True


def load_to_back(backend_url, swhrepo):
    """Load to the backend_url the repository swhrepo.
    """
    # First, store/retrieve the origin identifier
    # FIXME: should be done by the cloner worker (which is not yet plugged on the
    # right swh db ftm)
    client.put(backend_url,
               obj_type=store.Type.origin,
               obj=swhrepo.get_origin())

    # let the backend and api discuss what's really needed
    # - first this worker sends the checksums
    # - then the backend answers the checksums it does not know
    # - then the worker sends only what the backend does not know per
    # object type basis
    store_objects(backend_url, store.Type.content, swhrepo.get_contents())
    # the contents could fail, still we can continue with directories

    res = store_objects(backend_url, store.Type.directory, swhrepo.get_directories())
    if res:
        res = store_objects(backend_url, store.Type.revision, swhrepo.get_revisions())
        if res:
            # brutally send all remaining occurrences
            client.put(backend_url,
                       store.Type.occurrence,
                       swhrepo.get_occurrences())

            # and releases (the idea here is that compared to existing other objects,
            # the quantity is less)
            client.put(backend_url,
                       store.Type.release,
                       swhrepo.get_releases())
