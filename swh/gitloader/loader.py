# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import pygit2

from pygit2 import GIT_REF_OID
from pygit2 import GIT_FILEMODE_TREE, GIT_FILEMODE_COMMIT, GIT_OBJ_COMMIT
from swh import hash, db
from swh.gitloader import storage, models


def load_repo(db_conn,
              repo_path,
              file_content_storage_dir,
              object_content_storage_dir,
              folder_depth,
              blob_compress_flag):
    """Parse git repository `repo_path` and flush
    blobs on disk in `file_content_storage_dir`.
    """
    in_cache_objects = lambda *args: models.find_object(*args) is not None

    in_cache_blobs = lambda *args: models.find_blob(*args) is not None

    def store_object(object_ref, object_sha1_bin, object_type):
        """Store object in swh storage"""
        try:
            logging.debug('store %s %s' % (object_ref.hex, object_type))
            storage.add_object(object_content_storage_dir, object_ref,
                               folder_depth)
            models.add_object(db_conn, object_sha1_bin, object_type)
            db_conn.commit()
        except IOError:
            logging.error('store %s %s' % (object_ref.hex, object_type))
            db_conn.rollback()

    def store_blob(blob_entry_ref, blob_data_sha1_hex, blob_data_sha1_bin):
        """Store blob in swh storage."""
        try:
            logging.debug('store blob %s' % blob_entry_ref.hex)
            storage.add_blob(file_content_storage_dir,
                             blob_entry_ref.data,
                             blob_data_sha1_hex,
                             folder_depth,
                             blob_compress_flag)
            models.add_blob(db_conn,
                            blob_data_sha1_bin,
                            blob_entry_ref.size,
                            hash.sha1_bin(blob_entry_ref.hex))
            db_conn.commit()
        except IOError:
            logging.error('store blob %s' % blob_entry_ref.hex)
            db_conn.rollback()

    def walk_tree(repo, tree_ref):
        """Given a tree, walk the tree and save the blobs in file content storage
        (if not already present).
        """
        tree_sha1_bin = hash.sha1_bin(tree_ref.hex)

        if in_cache_objects(db_conn, tree_sha1_bin, models.Type.tree):
            logging.debug('skip tree %s' % tree_ref.hex)
            return

        for tree_entry in tree_ref:
            filemode = tree_entry.filemode

            if (filemode == GIT_FILEMODE_COMMIT):  # submodule!
                logging.warn('skip submodule-commit %s'
                             % tree_entry.id)
                continue

            elif (filemode == GIT_FILEMODE_TREE):  # Tree
                logging.debug('walk Tree %s'
                              % tree_entry.id)
                walk_tree(repo, repo[tree_entry.id])

            else:  # blob
                blob_entry_ref = repo[tree_entry.id]
                hashkey = hash.hashkey_sha1(blob_entry_ref.data)
                blob_data_sha1_bin = hashkey.digest()

                if in_cache_blobs(db_conn, blob_data_sha1_bin):
                    logging.debug('skip blob %s' % blob_entry_ref.hex)
                    continue

                store_blob(blob_entry_ref,
                           hashkey.hexdigest(),
                           blob_data_sha1_bin)

        store_object(tree_ref,
                     tree_sha1_bin,
                     models.Type.tree)

    def walk_revision_from(repo, head_commit):
        """Walk the revision from commit head_commit.
        """
        to_visits = [head_commit]  # the nodes to visit.
        visited = []               # the node visited and ready to be stored

        while to_visits:
            commit = to_visits.pop()
            if commit.type is not GIT_OBJ_COMMIT:
                continue

            commit_sha1_bin = hash.sha1_bin(commit.hex)
            if not in_cache_objects(db_conn, commit_sha1_bin, models.Type.commit):
                to_visits.extend(commit.parents)
                visited.append((commit_sha1_bin, commit))

        while visited:
            commit_sha1_bin, commit_to_store = visited.pop()
            if not in_cache_objects(db_conn, commit_sha1_bin, models.Type.commit):
                walk_tree(repo, commit_to_store.tree)
                store_object(commit_to_store,
                             commit_sha1_bin,
                             models.Type.commit)


    def walk_references_from(repo):
        """Walk the references from the repository repo_path.
        """
        for ref_name in repo.listall_references():
            logging.info('walk reference %s' % ref_name)
            ref = repo.lookup_reference(ref_name)
            head_commit = repo[ref.target] \
                              if ref.type is GIT_REF_OID \
                              else ref.peel(GIT_OBJ_COMMIT)
            walk_revision_from(repo, head_commit)

    walk_references_from(pygit2.Repository(repo_path))


def run(conf):
    """loader driver, dispatching to the relevant action

    used configuration keys:
    - action: requested action
    - repository: git repository path ('load' action only)
    - file_content_storage_dir: path to file content storage
    - object_content_storage_dir: path to git object content storage
    """
    with db.connect(conf['db_url']) as db_conn:
        action = conf['action']

        if action == 'cleandb':
            logging.info('clean database')
            models.cleandb(db_conn)
        elif action == 'initdb':
            logging.info('initialize database')
            models.initdb(db_conn)
        elif action == 'load':
            logging.info('load repository %s' % conf['repository'])
            load_repo(db_conn,
                      conf['repository'],
                      conf['file_content_storage_dir'],
                      conf['object_content_storage_dir'],
                      conf['folder_depth'],
                      conf['blob_compression'])
        else:
            logging.warn('skip unknown-action %s' % action)
