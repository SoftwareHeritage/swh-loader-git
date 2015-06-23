# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import pygit2

from pygit2 import GIT_REF_OID
from pygit2 import GIT_FILEMODE_TREE, GIT_FILEMODE_COMMIT, GIT_OBJ_COMMIT

from swh import hash
from swh.storage import models
from swh.http import client


def load_repo(baseurl,
              repo_path,
              file_content_storage_dir,
              object_content_storage_dir,
              folder_depth,
              blob_compress_flag):
    """Parse git repository `repo_path` and flush
    blobs on disk in `file_content_storage_dir`.
    """
    def store_object(object_ref, object_type):
        """Store object in swh storage"""
        logging.debug('store %s %s' % (object_ref.hex, object_type))
        client.put(baseurl, object_type, object_ref.hex,
                   data={'content': object_ref.read_raw()})

    def store_blob(blob_entry_ref, blob_data_sha1_hex):
        """Store blob in swh storage."""
        logging.debug('store blob %s' % blob_entry_ref)
        client.put(baseurl,
                   models.Type.blob,
                   blob_data_sha1_hex,
                   {'size': blob_entry_ref.size,
                    'git-sha1': blob_entry_ref.hex,
                    'content': blob_entry_ref.data})


    def store_commit(repo, commit_to_store):
        """Store a commit in swh storage.
        """
        store_tree(repo, commit_to_store.tree)
        store_object(commit_to_store,
                     models.Type.commit)


    def store_tree(repo, tree_ref):
        """Given a tree, walk the tree and save the blobs in file content storage
        (if not already present).
        """
        tree_sha1_hex = tree_ref.hex

        if client.get(baseurl, models.Type.tree, tree_sha1_hex):
            logging.debug('skip tree %s' % tree_sha1_hex)
            return

        for tree_entry in tree_ref:
            filemode = tree_entry.filemode
            tree_id = tree_entry.id

            if (filemode == GIT_FILEMODE_COMMIT):  # submodule!
                logging.warn('skip submodule-commit %s'
                             % tree_id)
                continue

            elif (filemode == GIT_FILEMODE_TREE):  # Tree
                logging.debug('walk tree %s'
                              % tree_id)
                store_tree(repo, repo[tree_id])

            else:  # blob
                blob_entry_ref = repo[tree_id]
                hashkey = hash.hashkey_sha1(blob_entry_ref.data)
                blob_data_sha1_hex = hashkey.hexdigest()

                if client.get(baseurl, models.Type.blob, blob_data_sha1_hex):
                    logging.debug('skip blob %s' % blob_entry_ref.hex)
                    continue

                store_blob(blob_entry_ref,
                           blob_data_sha1_hex)

        store_object(tree_ref,
                     models.Type.tree)

    def walk_revision_from(repo, head_commit, visited):
        """Walk the revision from commit head_commit.
        - repo is the current repository
        - head_commit is the latest commit to start from
        - visited is a memory cache of visited node (implemented as set)
        """
        to_visits = [head_commit]  # the nodes to visit topologically
        to_store = []              # the nodes to store in files + db

        while to_visits:
            commit = to_visits.pop()

            if commit.type is not GIT_OBJ_COMMIT:
                continue

            commit_sha1_hex = commit.hex
            if commit_sha1_hex not in visited \
               and not client.get(baseurl, models.Type.commit, commit_sha1_hex):
                visited.add(commit_sha1_hex)
                to_visits.extend(commit.parents)
                to_store.append(commit)

        while to_store:
            store_commit(repo, to_store.pop())

    def walk_references_from(repo):
        """Walk the references from the repository repo_path.
        """
        visited = set()  # global set of visited commits from such repository

        for ref_name in repo.listall_references():
            logging.info('walk reference %s' % ref_name)
            ref = repo.lookup_reference(ref_name)
            head_commit = repo[ref.target] \
                              if ref.type is GIT_REF_OID \
                              else ref.peel(GIT_OBJ_COMMIT)
            walk_revision_from(repo, head_commit, visited)

    walk_references_from(pygit2.Repository(repo_path))


def load(conf):
    """According to action, load the repository.

    used configuration keys:
    - action: requested action
    - repository: git repository path ('load' action only)
    - file_content_storage_dir: path to file content storage
    - object_content_storage_dir: path to git object content storage
    """
    action = conf['action']

    if action == 'load':
        logging.info('load repository %s' % conf['repository'])
        load_repo(conf['backend_url'],
                  conf['repository'],
                  conf['file_content_storage_dir'],
                  conf['object_content_storage_dir'],
                  conf['folder_depth'],
                  conf['blob_compression'])
    else:
        logging.warn('skip unknown-action %s' % action)
