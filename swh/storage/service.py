# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage import store, mapping


filter_unknowns_type = store.find_unknowns


def find_origin(db_conn, origin):
    """Find origin.
    """
    origin_found = store.find_origin(db_conn, origin)
    if origin_found:
        return {'id': origin_found[0]}
    return None


def add_origin(db_conn, origin):
    """Add origin if not already existing.
    """
    origin_found = store.find_origin(db_conn, origin)
    if origin_found:
        return {'id': origin_found[0]}
    else:
        origin_id = store.add_origin(db_conn, origin)
        return {'id': origin_id}


build_object_fn = {store.Type.revision: mapping.build_revision,
                   store.Type.directory: mapping.build_directory,
                   store.Type.content: mapping.build_content,
                   store.Type.release: mapping.build_release,
                   store.Type.occurrence: mapping.build_occurrence}


def add_revisions(db_conn, conf, obj_type, objs):
    """Add Revisions.
    """
    couple_parents = []
    for obj in objs:  # iterate over objects of type uri_type
        objfull = build_object_fn[obj_type](obj['sha1'], obj)

        obj_found = store.find(db_conn, objfull)
        if not obj_found:
            store.add(db_conn, conf, objfull)

            # deal with revision history
            par_shas = objfull.get('parent-sha1s', None)
            if par_shas:
                couple_parents.extend([(objfull['sha1'], p) for p in par_shas])

    store.add_revision_history(db_conn, couple_parents)

    return True


def add_objects(db_conn, conf, obj_type, objs):
    """Add objects.
    """
    for obj in objs:  # iterate over objects of type uri_type
        obj_to_store = build_object_fn[obj_type](obj['sha1'], obj)

        obj_found = store.find(db_conn, obj_to_store)
        if not obj_found:
            store.add(db_conn, conf, obj_to_store)

    return True
