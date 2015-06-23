# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh import hash
from swh.storage import db, models, fs


Type = models.Type


def hex_to_bin(sha1_hex):
    """Given an hexadecimal sha1, return its binary equivalent.
    Return None if sha1_hex is not the right sha1."""
    try:
        return hash.sha1_bin(sha1_hex)
    except:
        return None


_find_fn = {Type.blob: models.find_blob,
            Type.commit: models.find_object,
            Type.tree: models.find_object}


def find(config, git_object):
    """Find an object according to its sha1_hex and type.
    """
    sha1_hex = git_object['sha1']
    type = git_object['type']

    sha1_bin = hex_to_bin(sha1_hex)
    if sha1_bin is None:
        return None

    with db.connect(config['db_url']) as db_conn:
        return _find_fn[type](db_conn, sha1_bin, type)


def _add_blob(db_conn, config, git_object, sha1_bin):
    """Add a blob to storage.
    Designed to be wrapped in a db transaction.
    Returns:
    - True if everything went alright.
    - False if something went wrong during writing.
    - None if the git sha1 was not rightly formatted.
    Writing exceptions can also be raised and expected to be handled by the caller.
    """
    obj_git_sha1 = git_object['git-sha1']
    obj_git_sha_bin = hex_to_bin(obj_git_sha1)
    if obj_git_sha_bin is None:
        return None
    res = fs.write_object(config['file_content_storage_dir'],
                    obj_git_sha1,
                    git_object['content'],
                    config['folder_depth'],
                    config['blob_compression'])
    if res is not None:
        models.add_blob(db_conn, sha1_bin, git_object['size'], obj_git_sha_bin)
        return True
    return False


def _add_object(db_conn, config, git_object, sha1_bin):
    """Add a commit/tree to storage.
    Designed to be wrapped in a db transaction.
    Returns:
    - True if everything went alright.
    - False if something went wrong during writing
    Writing exceptions can also be raised and expected to be handled by the
    caller.
    """
    folder_depth = config['folder_depth']
    res = fs.write_object(config['object_content_storage_dir'],
                    git_object['sha1'],
                    git_object['content'],
                    folder_depth)
    if res is not None:
        models.add_object(db_conn, sha1_bin, git_object['type'])
        return True
    return False

_store_fn = {Type.blob: _add_blob,
             Type.commit: _add_object,
             Type.tree: _add_object}

def add(config, git_object):
    """Given a sha1_hex, type and content, store a given object in the store.
    """
    type = git_object['type']
    sha1_hex = git_object['sha1']
    sha1_bin = hex_to_bin(sha1_hex)
    if sha1_bin is None:
        return None

    with db.connect(config['db_url']) as db_conn:
        try:
            return _store_fn[type](db_conn, config, git_object, sha1_bin)
        except:  # all kinds of error break the transaction
            db_conn.rollback()
            return False
