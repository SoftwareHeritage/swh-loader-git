# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.git.storage import storage
from swh.loader.git.client import http


def store_unknown_objects(back_url, obj_type, swhmap):
    """Load objects to the backend.

    """
    sha1s = list(swhmap.keys())
    # have: filter unknown obj
    unknown_obj_sha1s = http.post(back_url, obj_type, sha1s)
    if not unknown_obj_sha1s:
        return True

    # store unknown objects
    return http.put(back_url, obj_type, map(swhmap.get, unknown_obj_sha1s))


def load_to_back(back_url, swh_repo):
    """Load to the back_url the repository swh_repo.

    """
    # First, store/retrieve the origin identifier
    # FIXME: should be done by the cloner worker (which is not yet plugged on
    # the right swh db ftm)
    http.put(back_url,
             obj_type=storage.Type.origin,
             obj=swh_repo.get_origin())

    http.put(back_url,
             obj_type=storage.Type.person,
             obj=list(swh_repo.get_persons()))

    # let the backend and api discuss what's really needed
    # - first this worker sends the checksums
    # - then the backend answers the checksums it does not know
    # - then the worker sends only what the backend does not know per
    # object type basis
    res = store_unknown_objects(back_url, storage.Type.content,
                                swh_repo.get_contents())

    if res:
        res = store_unknown_objects(back_url, storage.Type.directory,
                                    swh_repo.get_directories())
        if res:
            res = store_unknown_objects(back_url, storage.Type.revision,
                                        swh_repo.get_revisions())
            if res:
                # brutally send all remaining occurrences
                http.put(back_url,
                         storage.Type.occurrence,
                         swh_repo.get_occurrences())

                # and releases (the idea here is that compared to existing
                # other objects, the quantity is less)
                http.put(back_url,
                         storage.Type.release,
                         swh_repo.get_releases())

    # FIXME: deal with collision failures which should be raised by backend.
