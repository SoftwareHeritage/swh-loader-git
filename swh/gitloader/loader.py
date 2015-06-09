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
    return repo.walk(commit.id, pygit2.GIT_SORT_TOPOLOGICAL)


def _hashkeybin(data):
    """Given some data, compute the sha1 ready object of such data.
    Return the reference but not the computation.
    """
    return data.encode('utf-8')


def in_cache_objects(db_conn, sha):
    """Determine if an object with hash sha is in the cache.
    """
    return models.find_object(db_conn, sha) is not None


def add_object_in_cache(db_conn, obj, obj_type):
    """Add obj in cache.
    """
    sha1bin = _hashkeybin(obj.hex)

    if in_cache_objects(db_conn, sha1bin):
        logging.debug('Object \'%s\' already present... skip' % sha1bin)
        return

    logging.debug('Injecting object \'%s\' in cache' % sha1bin)

    models.add_object(db_conn, sha1bin, obj_type)


def _hashkey256(data):
    """Given some data, compute the hash ready object of such data.
    Return the reference but not the computation.
    """
    sha1 = hashlib.sha1()
    sha1.update(data)
    return sha1


def in_cache_blobs(db_conn, blob, hashkey=None):
    """Determine if a blob is in the blob cache.
    """
    hashkey = _hashkey256(blob.data).digest() if hashkey is None else hashkey
    return models.find_blob(db_conn, hashkey) is not None


def add_blob_in_cache(db_conn, blob, filepath):
    """Add blob in cache.
    """
    hashkey = _hashkey256(blob.data).digest()

    if in_cache_blobs(db_conn, blob, hashkey):
        logging.debug('Blob \'%s\' already present. skip' % blob.hex)
        return

    logging.debug('Injecting blob \'%s\' with \'%s\' (sha1: \'%s\')',
                  blob.hex,
                  filepath,
                  hashkey)

    models.add_blob(db_conn, hashkey, filepath)


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


def add_blob_in_file_storage(db_conn, file_content_storage_dir, blob):
    """Add blob in the file content storage (on disk).

TODO: split in another module, file manipulation maybe?
    """
    hashkey = _hashkey256(blob.data).hexdigest()
    folder_in_storage = create_dir_from_hash(file_content_storage_dir, hashkey)

    filepath = os.path.join(folder_in_storage, hashkey)
    logging.debug("Injecting blob '%s' in file content storage." % filepath)

    write_blob_on_disk(blob, filepath)

    return filepath


# Default types
TYPES = {"Tag": 3,
         "Blob": 2,
         "Tree": 1,
         "Commit": 0}


def parse_git_repo(db_conn,
                   repo_path,
                   file_content_storage_dir,
                   object_content_storage_dir):
    """Parse git repository `repo_path` and flush
    blobs on disk in `file_content_storage_dir`.
    """
    def _store_blobs_from_tree(tree_ref, repo):
        """Given a tree, walk the tree and store the blobs in file content storage
        (if not already present).
        """

        if in_cache_objects(db_conn, tree_ref.hex):
            logging.debug("Tree \'%s\' already visited, skip!" % tree_ref.hex)
            return

        # Add the tree in cache
        add_object_in_cache(db_conn, tree_ref,
                            TYPES["Tree"])

        # Now walk the tree
        for tree_entry in tree_ref:
            filemode = tree_entry.filemode

            if (filemode == pygit2.GIT_FILEMODE_COMMIT):  # submodule!
                logging.warn("Submodule - Key \'%s\' not found!"
                             % tree_entry.id)
                break

            elif (filemode == pygit2.GIT_FILEMODE_TREE):  # Tree
                logging.debug("Tree \'%s\' -> walk!"
                              % tree_entry.id)
                _store_blobs_from_tree(repo[tree_entry.id], repo)

            else:
                object_entry_ref = repo[tree_entry.id]

                # Remains only Blob
                if in_cache_blobs(db_conn, object_entry_ref):
                    logging.debug('Existing blob \'%s\' -> skip' %
                                  object_entry_ref.hex)
                    continue

                logging.debug("New blob \'%s\' -> in file storage!" %
                              object_entry_ref.hex)
                filepath = add_blob_in_file_storage(
                    db_conn,
                    file_content_storage_dir,
                    object_entry_ref)
                # add the file to the file cache, pointing to the file
                # path on the filesystem
                add_blob_in_cache(db_conn, object_entry_ref, filepath)

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
            if in_cache_objects(db_conn, commit.hex):
                break  # stop treating the current commit sub-graph
            else:
                add_object_in_cache(db_conn, commit,
                                    TYPES["Commit"])

                _store_blobs_from_tree(commit.tree, repo)


def run(actions, db_url,
        repo_path=None,
        file_content_storage_dir=None,
        object_content_storage_dir=None):
    """Parse a given git repository.
actions: CSV values amongst [initdb|cleandb]
repo_path: Path to the git repository
file_content_storage_dir: The folder where to store the raw blobs
object_content_storage_dir: The folder where to store the remaining git objects
    """
    db_conn = db_utils.db_connect(db_url)

    for action in actions:
        if action == 'cleandb':
            logging.info("Database cleanup!")
            models.cleandb(db_conn)
        elif action == 'initdb':
            logging.info("Database initialization!")
            models.initdb(db_conn)
        else:
            logging.warn("Unknown action '%s', skip!" % action)

    if repo_path is not None:
        logging.info("Parsing git repository \'%s\'" % repo_path)
        parse_git_repo(db_conn,
                       repo_path,
                       file_content_storage_dir,
                       object_content_storage_dir)

    db_conn.close()
