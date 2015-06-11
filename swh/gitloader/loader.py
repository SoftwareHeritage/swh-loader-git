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
add_object_in_cache = lambda *args: models.add_object(*args)


def load_repo(db_conn,
              repo_path,
              file_content_storage_dir,
              object_content_storage_dir):
    """Parse git repository `repo_path` and flush
    blobs on disk in `file_content_storage_dir`.
    """
    def save_blobs(tree_ref, repo):
        """Given a tree, walk the tree and store the blobs in file content storage
        (if not already present).
        """

        tree_sha1_bin = hash.sha1_bin(tree_ref.hex)

        if in_cache_objects(db_conn, tree_sha1_bin, models.Type.tree):
            logging.debug('Tree %s already visited, skip!' % tree_ref.hex)
            return

        # Add the tree in cache
        logging.debug('Store new tree %s (db).' % tree_sha1_bin)
        add_object_in_cache(db_conn, tree_sha1_bin, models.Type.tree)

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
                save_blobs(repo[tree_entry.id], repo)

            else:
                blob_entry_ref = repo[tree_entry.id]
                blob_data = blob_entry_ref.data
                hashkey = hash.hashkey_sha1(blob_data)
                blob_data_sha1_bin = hashkey.digest()

                # Remains only Blob
                if in_cache_blobs(db_conn, blob_data_sha1_bin):
                    logging.debug('Existing blob %s -> skip' %
                                  blob_entry_ref.hex)
                    continue

                logging.debug('Store new blob %s (db + file storage)!' %
                              blob_entry_ref.hex)
                storage.add_blob_to_storage(db_conn,
                                            file_content_storage_dir,
                                            blob_data,
                                            hashkey.hexdigest())

                models.add_blob(db_conn,
                                blob_data_sha1_bin,
                                blob_entry_ref.size,
                                hash.sha1_bin(blob_entry_ref.hex))

    repo = pygit2.Repository(repo_path)
    all_refs = repo.listall_references()

    # for each ref in the repo
    for ref_name in all_refs:
        logging.info('Parse reference %s' % ref_name)
        ref = repo.lookup_reference(ref_name)
        head_commit = ref.peel(pygit2.GIT_OBJ_COMMIT)
        # for each commit referenced by the commit graph starting at that ref
        for commit in repo.walk(head_commit.id, pygit2.GIT_SORT_TOPOLOGICAL):
            commit_sha1_bin = hash.sha1_bin(commit.hex)
            # if we have a git commit cache and the commit is in there:
            if in_cache_objects(db_conn, commit_sha1_bin, models.Type.commit):
                continue  # stop treating the current commit sub-graph
            else:
                logging.debug('Visit and store new commit %s (db).'
                              % commit_sha1_bin)

                add_object_in_cache(db_conn, commit_sha1_bin,
                                    models.Type.commit)
                save_blobs(commit.tree, repo)


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
            logging.info('Database cleanup!')
            models.cleandb(db_conn)
        elif action == 'initdb':
            logging.info('Database initialization!')
            models.initdb(db_conn)
        elif action == 'load':
            logging.info('Loading git repository %s' % conf['repository'])
            load_repo(db_conn,
                      conf['repository'],
                      conf['file_content_storage_dir'],
                      conf['object_content_storage_dir'])
        else:
            logging.warn('Unknown action %s, skip!' % action)
