# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import pygit2
import hashlib

from swh import db_utils
from swh.gitloader import models


def load_repo(parent_repo_path):
    """Load the repository path.
    """
    repo_path = pygit2.discover_repository(parent_repo_path)

    return pygit2.Repository(repo_path)


def commits_from(repo, commit):
    """Return the lists of commits from a given commit.
    """
    return repo.walk(commit.id, pygit2.GIT_SORT_TIME)


def _hashkey(data):
    """Given some data, compute the sha256 of such data.
    """
    sha256 = hashlib.sha256()
    sha256.update(data)
    return sha256.hexdigest()


def in_cache_objects(db_conn, obj):
    """Determine if a commit is in the cache.
    """
    return models.find_object(db_conn, obj.hex) is not None


def add_object_in_cache(db_conn, obj, obj_type):
    """Add obj in cache.
    """
    if in_cache_objects(db_conn, obj):
        logging.debug('Object \'%s\' already present... skip' % obj.hex)
        return

    logging.debug('Injecting object \'%s\' in cache' % obj.hex)

    models.add_object(db_conn, obj.hex, obj_type)


def in_cache_files(db_conn, blob, hashkey=None):
    """Determine if a file is in the file cache.
    """
    hashkey = _hashkey(blob.data) if hashkey is None else hashkey
    return models.find_file(db_conn, hashkey) is not None


def add_file_in_cache(db_conn, blob, filepath):
    """Add file in cache.
    """
    hashkey = _hashkey(blob.data)

    if in_cache_files(db_conn, blob, hashkey):
        logging.debug('Blob \'%s\' already present. skip' % blob.hex)
        return

    logging.debug('Injecting file \'%s\' with \'%s\' (sha256: \'%s\')',
                  blob.hex,
                  filepath,
                  hashkey)

    models.add_file(db_conn, hashkey, filepath)


def write_blob_on_disk(blob, filepath):
    """Write blob on disk.
    """
    f = open(filepath, 'wb')
    f.write(blob.data)
    f.close()


def create_dir_from_hash(dataset_dir, hash):
    """Create directory from a given hash.
    """
    def _compute_folder_name(dataset_dir):
        """Compute the folder prefix from a hash key.
        """
        # FIXME: find some split function
        return os.path.join(dataset_dir,
                            hash[0:2],
                            hash[2:4],
                            hash[4:6],
                            hash[6:8])

    folder_in_dataset = _compute_folder_name(dataset_dir)

    os.makedirs(folder_in_dataset, exist_ok=True)

    return folder_in_dataset


def add_file_in_dataset(db_conn, dataset_dir, blob):
    """Add file in the dataset (on disk).

TODO: split in another module, file manipulation maybe?
    """
    hashkey = _hashkey(blob.data)
    folder_in_dataset = create_dir_from_hash(dataset_dir, hashkey)

    filepath = os.path.join(folder_in_dataset, hashkey)
    logging.debug("Injecting file '%s' in dataset." % filepath)

    write_blob_on_disk(blob, filepath)

    return filepath


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

        if in_cache_objects(db_conn, tree_ref):
            logging.debug("Tree \'%s\' already visited, skip!" % tree_ref.hex)
            return

        # Add the tree in cache
        add_object_in_cache(db_conn, tree_ref,
                                   TYPES["Tree"])

        # Now walk the tree
        for tree_entry in tree_ref:
            object_entry_ref = repo[tree_entry.id]

            # FIXME: find pythonic way to do type dispatch call to function?
            if isinstance(object_entry_ref, pygit2.Tree):
                logging.debug("Tree \'%s\' -> walk!" % object_entry_ref.hex)
                _store_blobs_from_tree(object_entry_ref, repo)
            elif isinstance(object_entry_ref, pygit2.Blob):
                if in_cache_files(db_conn, object_entry_ref):
                    logging.debug('Blob \'%s\' already present. skip' %
                                  object_entry_ref.hex)
                    continue

                logging.debug("Blob \'%s\' -> store in dataset !" %
                              object_entry_ref.hex)
                # add the file to the dataset on the filesystem
                filepath = add_file_in_dataset(
                    db_conn,
                    dataset_dir,
                    object_entry_ref)
                # add the file to the file cache, pointing to the file
                # path on the filesystem
                add_file_in_cache(db_conn, object_entry_ref, filepath)
            else:
                logging.debug("Tag \'%s\' -> skip!" % object_entry_ref.hex)
                break

    repo = load_repo(repo_path)
    all_refs = repo.listall_references()

    # for each ref in the repo
    for ref_name in all_refs:
        logging.debug("Parse reference \'%s\' " % ref_name)
        ref = repo.lookup_reference(ref_name)
        head_commit = ref.peel()
        # for each commit referenced by the commit graph starting at that ref
        for commit in commits_from(repo, head_commit):
            # if we have a git commit cache and the commit is in there:
            if in_cache_objects(db_conn, commit):
                break  # stop treating the current commit sub-graph
            else:
                add_object_in_cache(db_conn, commit,
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
