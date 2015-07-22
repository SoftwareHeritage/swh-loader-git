# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from io import StringIO
from swh.storage import db, models, fs


Type = models.Type


_find_fn = {Type.content.value: models.find_blob,
            Type.revision.value: models.find_object,
            Type.directory.value: models.find_object}


def find(config, vcs_object):
    """Find an object according to its sha1_hex and type.
    """
    sha1_hex = vcs_object['sha1']
    type = vcs_object['type']

    with db.connect(config['db_url']) as db_conn:
        return _find_fn[type](db_conn, sha1_hex, type)


def find_unknowns(config, sha1s_hex):
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

    with db.connect(config['db_url']) as db_conn:
        unknowns = models.find_unknowns(db_conn, cpy_data_buffer)
        cpy_data_buffer.close()
        return list(map(row_to_sha1, unknowns))


def _add_content(db_conn, config, vcs_object, sha1_hex):
    """Add a blob to storage.
    Designed to be wrapped in a db transaction.
    Returns:
    - the sha1 if everything went alright.
    - None if something went wrong
    Writing exceptions can also be raised and expected to be handled by the
    caller.
    """
    res = fs.write_object(config['content_storage_dir'],
                          sha1_hex,
                          vcs_object['content'],
                          config['folder_depth'],
                          config['storage_compression'])
    if res is not None:
        models.add_content(db_conn,
                           sha1_hex,
                           vcs_object['sha1'],
                           vcs_object['sha256'],
                           vcs_object['size'])
        return sha1_hex
    return None


def _add_directory(db_conn, config, vcs_object, sha1_hex):
    """Add a directory to storage.
    Designed to be wrapped in a db transaction.
    Returns:
    - the sha1 if everything went alright.
    - None if something went wrong
    Writing exceptions can also be raised and expected to be handled by the
    caller.
    """
    res = fs.write_object(config['content_storage_dir'],
                          sha1_hex,
                          vcs_object['content'],
                          config['folder_depth'],
                          config['storage_compression'])
    if res is not None:
        models.add_directory(db_conn,
                             sha1_hex,
                             vcs_object['target-sha1'],
                             vcs_object['nature'],  # dir or file
                             vcs_object['perms'],
                             vcs_object['atime'],
                             vcs_object['mtime'],
                             vcs_object['ctime'],
                             vcs_object['parent'])

        return sha1_hex
    return None


def _add_revision(db_conn, config, vcs_object, sha1_hex):
    """Add a revision to storage.
    Designed to be wrapped in a db transaction.
    Returns:
    - the sha1 if everything went alright.
    - None if something went wrong
    Writing exceptions can also be raised and expected to be handled by the
    caller.
    """
    res = fs.write_object(config['content_storage_dir'],
                          sha1_hex,
                          vcs_object['content'],
                          config['folder_depth'],
                          config['storage_compression'])

  if res is not None:
        models.add_revision(db_conn,
                            sha1_hex,
                            vcs_object['date'],
                            vcs_object['directory'],
                            vcs_object['message'],
                            vcs_object['author'],
                            vcs_object['committer'],
                            vcs_object['parent-id'])
        return sha1_hex
    return None

_store_fn = {Type.content.value: _add_content,
             Type.directory.value: _add_directory,
             Type.revision.value: _add_revision}


def add(config, vcs_object):
    """Given a sha1_hex, type and content, store a given object in the store.
    """
    type = vcs_object['type']
    sha1_hex = vcs_object['sha1']

    with db.connect(config['db_url']) as db_conn:
        try:
            return _store_fn[type](db_conn, config, vcs_object, sha1_hex)
        except:  # all kinds of error break the transaction
            db_conn.rollback()
            return False
