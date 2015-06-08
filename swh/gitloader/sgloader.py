# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import argparse
import configparser
import logging
import os

import pygit2

from swh import db_utils
from swh.gitloader import loader, models


# Default types
TYPES = {"Tag": 3,
         "Blob": 2,
         "Tree": 1,
         "Commit": 0}


def parse_git_repo(db_conn, repo_path, dataset_dir):
    """Parse git repository `repo_path` and flush files on disk in `dataset_dir`.
    """
    def _store_blobs_from_tree(tree_ref, repo):
        """Given a tree, walk the tree and store the blobs in dataset
 (if not present in dataset/cache).
        """

        if loader.in_cache_objects(db_conn, tree_ref):
            logging.debug("Tree \'%s\' already visited, skip!" % tree_ref.hex)
            return

        # Add the tree in cache
        loader.add_object_in_cache(db_conn, tree_ref,
                                   TYPES["Tree"])

        # Now walk the tree
        for tree_entry in tree_ref:
            object_entry_ref = repo[tree_entry.id]

            # FIXME: find pythonic way to do type dispatch call to function?
            if isinstance(object_entry_ref, pygit2.Tree):
                logging.debug("Tree \'%s\' -> walk!" % object_entry_ref.hex)
                _store_blobs_from_tree(object_entry_ref, repo)
            elif isinstance(object_entry_ref, pygit2.Blob):
                if loader.in_cache_files(db_conn, object_entry_ref):
                    logging.debug('Blob \'%s\' already present. skip' %
                                  object_entry_ref.hex)
                    continue

                logging.debug("Blob \'%s\' -> store in dataset !" %
                              object_entry_ref.hex)
                # add the file to the dataset on the filesystem
                filepath = loader.add_file_in_dataset(
                    db_conn,
                    dataset_dir,
                    object_entry_ref)
                # add the file to the file cache, pointing to the file
                # path on the filesystem
                loader.add_file_in_cache(db_conn, object_entry_ref, filepath)
            else:
                logging.debug("Tag \'%s\' -> skip!" % object_entry_ref.hex)
                break

    repo = loader.load_repo(repo_path)
    all_refs = repo.listall_references()

    # for each ref in the repo
    for ref_name in all_refs:
        logging.debug("Parse reference \'%s\' " % ref_name)
        ref = repo.lookup_reference(ref_name)
        head_commit = ref.peel()
        # for each commit referenced by the commit graph starting at that ref
        for commit in loader.commits_from(repo, head_commit):
            # if we have a git commit cache and the commit is in there:
            if loader.in_cache_objects(db_conn, commit):
                break  # stop treating the current commit sub-graph
            else:
                loader.add_object_in_cache(db_conn, commit,
                                           TYPES["Commit"])
                _store_blobs_from_tree(commit.tree, repo)


def run(action, db_url, repo_path = None, dataset_dir = None):
    db_conn = db_utils.db_connect(db_url)

    if action == 'cleandb':
        logging.info("Database cleanup!")
        models.cleandb(db_conn)
    else:
        if action == 'initdb':
            logging.info("Database initialization!")
            models.initdb(db_conn)

        logging.info("Parsing git repository \'%s\'" % repo_path)
        parse_git_repo(db_conn, repo_path, dataset_dir)

    db_conn.close()
