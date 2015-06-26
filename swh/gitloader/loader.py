# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import pygit2


from pygit2 import GIT_REF_OID
from pygit2 import GIT_OBJ_COMMIT, GIT_OBJ_TREE, GIT_OBJ_BLOB

from swh import hash
from swh.storage import store
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
                   store.Type.blob,
                   blob_data_sha1_hex,
                   {'size': blob_entry_ref.size,
                    'git-sha1': blob_entry_ref.hex,
                    'content': blob_entry_ref.data})

    def treewalk(repo, tree, topdown=True):
        """Walk a tree with the same implementation as `os.path`.
        Returns: tree, trees, blobs
        """
        trees, blobs = [], []
        for tree_entry in tree:
            obj = repo.get(tree_entry.oid, None)
            if obj is None:
                logging.warn('skip submodule-commit %s' % tree_entry.hex)
                continue  # submodule!

            if obj.type == GIT_OBJ_TREE:
                trees.append(obj)
            else:
                blobs.append(obj)

        if topdown:
            yield tree, trees, blobs
        for tree_entry in trees:
            for x in treewalk(repo, repo[tree_entry.oid], topdown):
                yield x
        if not topdown:
            yield tree, trees, blobs


    type_to = {GIT_OBJ_BLOB: store.Type.blob,
               GIT_OBJ_TREE: store.Type.tree,
               GIT_OBJ_COMMIT: store.Type.commit}

    def store_ref(sha1_hex, obj_ref):
        t = type_to[obj_ref.type]
        if t == store.Type.blob:
            store_blob(obj_ref, sha1_hex)
        else:
            store_object(obj_ref, t)

    def store_tree(repo, tree_ref):
        """Walk the tree and save the blobs in file content storage
        (if not already present).
        """
        # tree_sha1_hex = tree_ref.hex

        # if client.get(baseurl, store.Type.tree, tree_sha1_hex):
        #     logging.debug('skip tree %s' % tree_sha1_hex)
        #     return

        sha1s_hex = []
        sha1s_map = {}
        for ori_tree_ref, trees_ref, blobs_ref in treewalk(repo, tree_ref,
                                                           topdown=False):

            for blob_ref in blobs_ref:
                blob_data_sha1hex = hash.hashkey_sha1(blob_ref.data).hexdigest()
                sha1s_hex.append(blob_data_sha1hex)
                sha1s_map[blob_data_sha1hex] = blob_ref
                # store_blob(blob_ref, blob_data_sha1hex)

            for tree_ref in trees_ref:
                sha1s_hex.append(tree_ref.hex)
                sha1s_map[tree_ref.hex] = tree_ref
                # store_object(tree_ref, store.Type.tree)

            sha1s_hex.append(ori_tree_ref.hex)
            sha1s_map[ori_tree_ref.hex] = ori_tree_ref
            # store_object(ori_tree_ref, store.Type.tree)

        return sha1s_hex, sha1s_map

        # store_object(tree_ref, store.Type.tree)


    def store_commit(repo, commit_to_store):
        """Store a commit in swh storage.
        """
        sha1s_hex, sha1s_map = store_tree(repo, commit_to_store.tree)
        sha1s_hex.append(commit_to_store.hex)
        sha1s_map[commit_to_store.hex] = commit_to_store

        res = client.post(baseurl, {'sha1s': sha1s_hex})

        for unknown_ref_sha1 in res:
            store_ref(unknown_ref_sha1, sha1s_map[unknown_ref_sha1])


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
               and not client.get(baseurl, store.Type.commit, commit_sha1_hex):
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
                          else ref.peel(GIT_OBJ_COMMIT)  # noqa
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
