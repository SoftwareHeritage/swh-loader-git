# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.store import store


filter_unknowns_type = store.find_unknowns


def find_origin(db_conn, origin):
    """Find origin.
    """
    origin_found = store.find_origin(db_conn, origin)
    return None if not origin_found else {'id': origin_found[0]}


def find_person(db_conn, person):
    """Find person.
    """
    person_found = store.find_person(db_conn, person)
    return None if not person_found else {'id': person_found[0]}


def add_origin(db_conn, origin):
    """Add origin if not already existing.
    """
    origin_found = store.find_origin(db_conn, origin)
    id = origin_found[0] if origin_found else store.add_origin(db_conn, origin)
    return {'id': id}


def add_revisions(db_conn, conf, obj_type, objs):
    """Add Revisions.
    """
    couple_parents = []
    for obj in objs:  # iterate over objects of type uri_type
        obj_id = obj['id']
        obj_found = store.find(db_conn, obj_id, obj_type)
        if not obj_found:
            store.add(db_conn, conf, obj_id, obj_type, obj)

            # deal with revision history
            par_shas = obj.get('parent-sha1s', None)
            if par_shas:
                couple_parents.extend([(obj_id, p) for p in par_shas])

    store.add_revision_history(db_conn, couple_parents)

    return True


def add_persons(db_conn, conf, obj_type, objs):
    """Add persons.
    conf, obj_type are not used (implementation detail.)
    """
    for obj in objs:
        obj_found = store.find_person(db_conn, obj)
        if not obj_found:
            store.add_person(db_conn, obj)

    return True


# dispatch map to add in storage with fs or not
_add_fn = {store.Type.content: store.add_with_fs_storage}


def add_objects(db_conn, conf, obj_type, objs):
    """Add objects if not already present in the storage.
    """
    add_fn = _add_fn.get(obj_type, store.add)
    res = []
    for obj in objs:  # iterate over objects of type uri_type
        obj_id = obj['id']
        obj_found = store.find(db_conn, obj_id, obj_type)
        if not obj_found:
            obj = add_fn(db_conn, conf, obj_id, obj_type, obj)
            res.append(obj)
        else:
            res.append(obj_found)

    return res


_persist_fn = {store.Type.person: add_persons,
               store.Type.revision: add_revisions}


def persist(db_conn, conf, obj_type, objs):
    """Generic call to persist persons, revisions or other objects.

    """
    persist_fn = _persist_fn.get(obj_type, add_objects)
    return persist_fn(db_conn, conf, obj_type, objs)
