# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import pygit2

from swh import hash, db
from swh.gitloader import storage, models


in_cache_objects = lambda *args: models.find_object(*args) is not None
in_cache_blobs = lambda *args: models.find_blob(*args) is not None


def load_repo(db_conn,
              repo_path,
              file_content_storage_dir,
              object_content_storage_dir,
              folder_depth,
              blob_compress_flag):
    """Parse git repository `repo_path` and flush
    blobs on disk in `file_content_storage_dir`.
    """
    def walk_tree(tree_ref, repo):
        """Given a tree, walk the tree and save the blobs in file content storage
        (if not already present).
        """
        tree_sha1_bin = hash.sha1_bin(tree_ref.hex)

        if in_cache_objects(db_conn, tree_sha1_bin, models.Type.tree):
            logging.debug('skip tree %s' % tree_ref.hex)
            return

        # Add the tree in cache
        logging.debug('store tree %s' % tree_sha1_bin)
        storage.add_object(object_content_storage_dir, tree_ref, folder_depth)
        models.add_object(db_conn, tree_sha1_bin, models.Type.tree)
        db_conn.commit()

        # Now walk the tree
        for tree_entry in tree_ref:
            filemode = tree_entry.filemode

            if (filemode == pygit2.GIT_FILEMODE_COMMIT):  # submodule!
                logging.warn('skip submodule-commit %s'
                             % tree_entry.id)
                continue

            elif (filemode == pygit2.GIT_FILEMODE_TREE):  # Tree
                logging.debug('walk Tree %s'
                              % tree_entry.id)
                walk_tree(repo[tree_entry.id], repo)

            else:
                blob_entry_ref = repo[tree_entry.id]
                blob_data = blob_entry_ref.data
                hashkey = hash.hashkey_sha1(blob_data)
                blob_data_sha1_bin = hashkey.digest()

                # Remains only Blob
                if in_cache_blobs(db_conn, blob_data_sha1_bin):
                    logging.debug('skip blob %s' % blob_entry_ref.hex)
                    continue

                logging.debug('store blob %s' %
                              blob_entry_ref.hex)
                storage.add_blob(file_content_storage_dir,
                                 blob_data,
                                 hashkey.hexdigest(),
                                 folder_depth,
                                 blob_compress_flag)
                models.add_blob(db_conn,
                                blob_data_sha1_bin,
                                blob_entry_ref.size,
                                hash.sha1_bin(blob_entry_ref.hex))
                db_conn.commit()

    repo = pygit2.Repository(repo_path)
    all_refs = repo.listall_references()

    # for each ref in the repo
    for ref_name in all_refs:
        logging.info('walk reference %s' % ref_name)
        ref = repo.lookup_reference(ref_name)
        head_commit = ref.peel(pygit2.GIT_OBJ_COMMIT)
        # for each commit referenced by the commit graph starting at that ref
        for commit in repo.walk(head_commit.id, pygit2.GIT_SORT_TOPOLOGICAL):
            commit_sha1_bin = hash.sha1_bin(commit.hex)
            # if we have a git commit cache and the commit is in there:
            if in_cache_objects(db_conn, commit_sha1_bin, models.Type.commit):
                continue  # stop treating the current commit sub-graph
            else:
                logging.debug('store commit %s'
                              % commit_sha1_bin)

                storage.add_object(object_content_storage_dir, commit,
                                   folder_depth)
                models.add_object(db_conn, commit_sha1_bin,
                                  models.Type.commit)
                db_conn.commit()

                walk_tree(commit.tree, repo)


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
