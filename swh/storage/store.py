#!/usr/bin/env python3

# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh import hash
from swh.storage import db, models, fs


def find(config, git_object):
    """Find an object according to its sha1_hex and type.
    """
    sha1_hex = git_object['sha1']
    type = git_object['type']

    sha1_bin = hex_to_bin(sha1_hex)
    if sha1_bin is None:
        return None

    with db.connect(config['db_url']) as db_conn:
        if type is models.Type.blob:
            return models.find_blob(db_conn, sha1_bin)
        else:
            return models.find_object(db_conn, sha1_bin, type)

def hex_to_bin(sha1_hex):
    """Given an hexadecimal sha1, return its binary equivalent.
    Return None if sha1_hex is not the right sha1."""
    try:
        return hash.sha1_bin(sha1_hex)
    except:
        return None

def add(config, git_object):
    """Given a sha1_hex, type and content, store a given object in the store.
    """
    type = git_object['type']
    sha1_hex = git_object['sha1']
    sha1_bin = hex_to_bin(sha1_hex)
    if sha1_bin is None:
        return None

    content = git_object['content']

    db_url = config['db_url']
    storage_dir = config['file_content_storage_dir' if type is models.Type.blob else 'object_content_storage_dir']
    folder_depth = config['folder_depth']

    with db.connect(db_url) as db_conn:
        try:
            logging.debug('store %s %s' % (sha1_hex, type))
            if type is models.Type.blob:
                obj_git_sha = git_object['git-sha1']
                obj_git_sha_bin = hex_to_bin(obj_git_sha)
                if obj_git_sha_bin is None:
                    return None

                fs.write_object(storage_dir,
                                sha1_hex,
                                content,
                                folder_depth,
                                config['blob_compression'])

                # creation
                models.add_blob(db_conn, sha1_bin, git_object['size'], obj_git_sha_bin)
            else:
                fs.write_object(storage_dir,
                                sha1_hex,
                                content,
                                folder_depth)
                models.add_object(db_conn, sha1_bin, type)
            return True
        except IOError:
            db_conn.rollback()
            logging.error('store %s %s' % (sha1_hex, type))
            return False
