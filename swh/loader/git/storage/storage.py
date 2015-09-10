# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from io import StringIO

from . import models


Type = models.Type


_find_object = {Type.occurrence: models.find_occurrences_for_revision,
                Type.content: lambda *args: models.find_object(*args, column='sha1')}

def find(db_conn, id, type):
    """Find an object according to its sha1hex and type.
    """
    return _find_object.get(type, models.find_object)(db_conn, id, type)


_find_unknown = {Type.revision: models.find_unknown_revisions,
                 Type.content: models.find_unknown_contents,
                 Type.directory: models.find_unknown_directories}


def find_unknowns(db_conn, obj_type, sha1s_hex):
    """Given a list of sha1s, return the non presents one in storage.
    """
    def row_to_sha1(row):
        """Convert a row (memoryview) to a string sha1.
        """
        return row[0]

    vals = '\n'.join(sha1s_hex)
    cpy_data_buffer = StringIO()
    cpy_data_buffer.write(vals)
    cpy_data_buffer.seek(0)  # move file cursor back at start of file

    find_unknown_fn = _find_unknown[obj_type]
    unknowns = find_unknown_fn(db_conn, cpy_data_buffer)
    cpy_data_buffer.close()
    return list(map(row_to_sha1, unknowns))


def _add_content(db_conn, vcs_object, sha1hex):
    """Add a blob to storage.
    Designed to be wrapped in a db transaction.
    Returns:
    - the sha1 if everything went alright.
    - None if something went wrong
    Writing exceptions can also be raised and expected to be handled by the
    caller.
    """
    models.add_content(db_conn,
                       sha1hex,
                       vcs_object['git-sha1'],
                       vcs_object['content-sha256'],
                       vcs_object['size'])
    return sha1hex


def _add_directory(db_conn, vcs_object, sha1hex):
    """Add a directory to storage.
    Designed to be wrapped in a db transaction.
    """
    parent_id = models.add_directory(db_conn, sha1hex)
    for directory_entry_dir in vcs_object['entry-dirs']:
        _add_directory_entry_dir(db_conn, parent_id, directory_entry_dir)
    for directory_entry_file in vcs_object['entry-files']:
        _add_directory_entry_file(db_conn, parent_id, directory_entry_file)
    return sha1hex


def _add_directory_entry_dir(db_conn, parent_id, vcs_object):
    """Add a directory entry dir to storage.
    Designed to be wrapped in a db transaction.
    Returns:
    - the sha1 if everything went alright.
    - None if something went wrong
    Writing exceptions can also be raised and expected to be handled by the
    caller.
    """
    name = vcs_object['name']
    models.add_directory_entry_dir(db_conn,
                                   name,
                                   vcs_object['target-sha1'],
                                   vcs_object['perms'],
                                   vcs_object['atime'],
                                   vcs_object['mtime'],
                                   vcs_object['ctime'],
                                   parent_id)
    return name, parent_id


def _add_directory_entry_file(db_conn, parent_id, vcs_object):
    """Add a directory to storage.
    Designed to be wrapped in a db transaction.
    Returns:
    - the sha1 if everything went alright.
    - None if something went wrong
    Writing exceptions can also be raised and expected to be handled by the
    caller.
    """
    name = vcs_object['name']
    models.add_directory_entry_file(db_conn,
                                    name,
                                    vcs_object['target-sha1'],
                                    vcs_object['perms'],
                                    vcs_object['atime'],
                                    vcs_object['mtime'],
                                    vcs_object['ctime'],
                                    parent_id)
    return name, parent_id


def _add_revision(db_conn, vcs_object, sha1hex):
    """Add a revision to storage.
    Designed to be wrapped in a db transaction.
    Returns:
    - the sha1 if everything went alright.
    - None if something went wrong
    Writing exceptions can also be raised and expected to be handled by the
    caller.
    """
    models.add_revision(db_conn,
                        sha1hex,
                        vcs_object['date'],
                        vcs_object['directory'],
                        vcs_object['message'],
                        vcs_object['author'],
                        vcs_object['committer'],
                        vcs_object['parent-sha1s'])
    return sha1hex


def _add_release(db_conn, vcs_object, sha1hex):
    """Add a release.
    """
    models.add_release(db_conn,
                       sha1hex,
                       vcs_object['revision'],
                       vcs_object['date'],
                       vcs_object['name'],
                       vcs_object['comment'],
                       vcs_object['author'])
    return sha1hex


def _add_occurrence(db_conn, vcs_object, sha1hex):
    """Add an occurrence.
    """
    models.add_occurrence(db_conn,
                          vcs_object['url-origin'],
                          vcs_object['branch'],
                          vcs_object['revision'])
    return sha1hex


def add_person(db_conn, vcs_object):
    """Add an author.
    """
    return models.add_person(db_conn,
                             vcs_object['name'],
                             vcs_object['email'])


_store_fn = {Type.directory: _add_directory,
             Type.revision: _add_revision,
             Type.release: _add_release,
             Type.occurrence: _add_occurrence}


def add_origin(db_conn, origin):
    """A a new origin and returns its id.
    """
    return models.add_origin(db_conn, origin['url'], origin['type'])


def find_origin(db_conn, origin):
    """Find an existing origin.
    """
    return models.find_origin(db_conn, origin['url'], origin['type'])


def find_person(db_conn, person):
    """Find an existing person.
    """
    return models.find_person(db_conn, person['email'], person['name'])


def add_with_fs_storage(db_conn, config, id, type, vcs_object):
    """Add vcs_object in the storage
    - db_conn is the opened connection to the db
    - config is the map of configuration needed for core layer
    - type is not used here but represent the type of vcs_object
    - vcs_object is the object meant to be persisted in fs and db
    """
    id_ = config['objstorage'].add_bytes(vcs_object['content'])
    if id_ != id:
        raise Exception("The ids should have been identical. Corruption.")

    return _add_content(db_conn, vcs_object, id)


def add(db_conn, config, id, type, vcs_object):
    """Given a sha1hex, type and content, store a given object in the store.
    - db_conn is the opened connection to the db
    - config is not used here
    - type is the object's type
    - vcs_object is the object meant to be persisted in db
    """
    return _store_fn[type](db_conn, vcs_object, id)


def add_revision_history(db_conn, couple_parents):
    """Given a list of tuple (sha, parent_sha), store in revision_history.
    """
    if len(couple_parents) > 0:
        models.add_revision_history(db_conn, couple_parents)
