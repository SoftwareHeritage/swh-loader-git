# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from io import StringIO
from swh.storage import db, models, fs


Type = models.Type

_find_object = {Type.occurrence: models.find_occurrences_for_revision}


def find(db_conn, vcs_object):
    """Find an object according to its sha1hex and type.
    """
    id = vcs_object['sha1']   # sha1 for every object except for origin (url)
    type = vcs_object['type']

    find_fn  = _find_object.get(type, models.find_object)
    return find_fn(db_conn, id, type)


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
                       vcs_object['content-sha1'],
                       vcs_object['content-sha256'],
                       vcs_object['size'])
    return sha1hex


def _add_directory(db_conn, vcs_object, sha1hex):
    """Add a directory to storage.
    Designed to be wrapped in a db transaction.
    """
    models.add_directory(db_conn, sha1hex)
    for directory_entry in vcs_object['entries']:
        _add_directory_entry(db_conn, directory_entry)
    return sha1hex


def _add_directory_entry(db_conn, vcs_object):
    """Add a directory to storage.
    Designed to be wrapped in a db transaction.
    Returns:
    - the sha1 if everything went alright.
    - None if something went wrong
    Writing exceptions can also be raised and expected to be handled by the
    caller.
    """
    name = vcs_object['name']
    parent = vcs_object['parent']
    models.add_directory_entry(db_conn,
                               name,
                               vcs_object['target-sha1'],
                               vcs_object['nature'],
                               vcs_object['perms'],
                               vcs_object['atime'],
                               vcs_object['mtime'],
                               vcs_object['ctime'],
                               parent)
    return name, parent


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
                          vcs_object['reference'],
                          vcs_object['revision'])
    return sha1hex


_store_fn = {Type.content:   _add_content,
             Type.directory: _add_directory,
             Type.revision:  _add_revision,
             Type.release:   _add_release,
             Type.occurrence: _add_occurrence}


def add_origin(db_conn, origin):
    """A a new origin and returns its id.
    """
    return models.add_origin(db_conn, origin['url'], origin['type'])


def find_origin(db_conn, origin):
    """Find an existing origin.
    """
    return models.find_origin(db_conn, origin['url'], origin['type'])


def add(db_conn, config, vcs_object):
    """Given a sha1hex, type and content, store a given object in the store.
    """
    type = vcs_object['type']
    sha1hex = vcs_object['sha1']
    obj_content = vcs_object.get('content')

    if obj_content:
        res = fs.write_object(config['content_storage_dir'],
                              sha1hex,
                              obj_content,
                              config['folder_depth'],
                              config['storage_compression'])
        if not res:
            return False
        return _store_fn[type](db_conn, vcs_object, sha1hex)
    return _store_fn[type](db_conn, vcs_object, sha1hex)


def add_revision_history(db_conn, couple_parents):
    """Given a list of tuple (sha, parent_sha), store in revision_history.
    """
    if len(couple_parents) > 0:
        models.add_revision_history(db_conn, couple_parents)
