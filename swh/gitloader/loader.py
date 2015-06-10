# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import pygit2
import hashlib
import binascii

from enum import Enum

from swh.gitloader import models


class Type(Enum):
    commit = 0
    tree = 1
    blob = 2
    tag = 3


in_cache_objects = lambda *args: models.find_object(*args) is not None
in_cache_blobs = lambda *args: models.find_blob(*args) is not None


def add_object_in_cache(db_conn, obj_sha, obj_type):
    """Add obj in cache.
    """
    logging.debug('Injecting object %s in cache' % obj_sha)
    models.add_object(db_conn, obj_sha, obj_type)


def write_blob_on_disk(blob, filepath):
    """Write blob on disk.
    """
    f = open(filepath, 'wb')
    f.write(blob.data)
    f.close()


def create_dir_from_hash(file_content_storage_dir, hash):
    """Create directory from a given hash.
    """
    def _compute_folder_name(file_content_storage_dir):
        """Compute the folder prefix from a hash key.
        """
        # FIXME: find some split function
        return os.path.join(file_content_storage_dir,
                            hash[0:2],
                            hash[2:4],
                            hash[4:6],
                            hash[6:8])

    folder_in_storage = _compute_folder_name(file_content_storage_dir)
    os.makedirs(folder_in_storage, exist_ok=True)

    return folder_in_storage


def add_blob_in_file_storage(db_conn, file_content_storage_dir, blob, hashkey):
    """Add blob in the file content storage (on disk).

TODO: split in another module, file manipulation maybe?
    """
    folder_in_storage = create_dir_from_hash(file_content_storage_dir, hashkey)
    filepath = os.path.join(folder_in_storage, hashkey)
    logging.debug('Injecting blob %s in file content storage.' % filepath)
    write_blob_on_disk(blob, filepath)
    return filepath


def _sha1_bin(hexsha1):
    """Compute the sha1's binary format from an hexadecimal format string.
    """
    return binascii.unhexlify(hexsha1)


def _hashkey_sha1(data):
    """Given some data, compute the hash ready object of such data.
    Return the reference but not the computation.
    """
    sha1 = hashlib.sha1()
    sha1.update(data)
    return sha1


def parse_git_repo(db_conn,
                   repo_path,
                   file_content_storage_dir,
                   object_content_storage_dir):
    """Parse git repository `repo_path` and flush
    blobs on disk in `file_content_storage_dir`.
    """
    def store_blobs_from_tree(tree_ref, repo):
        """Given a tree, walk the tree and store the blobs in file content storage
        (if not already present).
        """

        tree_sha1_bin = _sha1_bin(tree_ref.hex)

        if in_cache_objects(db_conn, tree_sha1_bin, Type.tree):
            logging.debug('Tree %s already visited, skip!' % tree_ref.hex)
            return

        # Add the tree in cache
        add_object_in_cache(db_conn, tree_sha1_bin, Type.tree)

        # Now walk the tree
        for tree_entry in tree_ref:
            filemode = tree_entry.filemode

            if (filemode == pygit2.GIT_FILEMODE_COMMIT):  # submodule!
                logging.warn('Submodule - Key %s not found!'
                             % tree_entry.id)
                continue

            elif (filemode == pygit2.GIT_FILEMODE_TREE):  # Tree
                logging.debug('Tree %s -> walk!'
                              % tree_entry.id)
                store_blobs_from_tree(repo[tree_entry.id], repo)

            else:
                blob_entry_ref = repo[tree_entry.id]
                hashkey = _hashkey_sha1(blob_entry_ref.data)
                blob_data_sha1_bin = hashkey.digest()

                # Remains only Blob
                if in_cache_blobs(db_conn, blob_data_sha1_bin):
                    logging.debug('Existing blob %s -> skip' %
                                  blob_entry_ref.hex)
                    continue

                logging.debug('New blob %s -> in file storage!' %
                              blob_entry_ref.hex)
                add_blob_in_file_storage(
                    db_conn,
                    file_content_storage_dir,
                    blob_entry_ref,
                    hashkey.hexdigest())

                models.add_blob(db_conn,
                                blob_data_sha1_bin,
                                blob_entry_ref.size,
                                _sha1_bin(blob_entry_ref.hex))

    repo = load_repo(repo_path)
    all_refs = repo.listall_references()

    # for each ref in the repo
    for ref_name in all_refs:
        logging.debug('Parse reference %s' % ref_name)
        ref = repo.lookup_reference(ref_name)
        head_commit = ref.peel()
        # for each commit referenced by the commit graph starting at that ref
        for commit in repo.walk(head_commit.id, pygit2.GIT_SORT_TOPOLOGICAL):
            commit_sha1_bin = _sha1_bin(commit.hex)
            # if we have a git commit cache and the commit is in there:
            if in_cache_objects(db_conn, commit_sha1_bin, Type.commit):
                continue  # stop treating the current commit sub-graph
            else:
                add_object_in_cache(db_conn, commit_sha1_bin,
                                    Type.commit)

                store_blobs_from_tree(commit.tree, repo)


def run(conf):
    """loader driver, dispatching to the relevant action

    used configuration keys:
    - action: requested action
    - repository: git repository path ('load' action only)
    - file_content_storage_dir: path to file content storage
    - object_content_storage_dir: path to git object content storage
    """

    db_conn = models.db_connect(conf['db_url'])
    action = conf['action']

    if action == 'cleandb':
        logging.info('Database cleanup!')
        models.cleandb(db_conn)
    elif action == 'initdb':
        logging.info('Database initialization!')
        models.initdb(db_conn)
    elif action == 'load':
        logging.info('Loading git repository %s' % conf['repository'])
        parse_git_repo(db_conn,
                       conf['repository'],
                       conf['file_content_storage_dir'],
                       conf['object_content_storage_dir'])
    else:
        logging.warn('Unknown action %s, skip!' % action)

    db_conn.close()
