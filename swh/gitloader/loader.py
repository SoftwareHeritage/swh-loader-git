# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import pygit2
import os

from pygit2 import GIT_REF_OID
from pygit2 import GIT_OBJ_COMMIT, GIT_OBJ_TREE

from swh import hash
from swh.storage import store
from swh.data import swhmap
from swh.http import client

def load_repo(baseurl, repo_path):
    """Parse git repository `repo_path` and discuss with backend to store the
    parsing result.
    """
    def treewalk(repo, tree, topdown=False):
        """Walk a tree with the same implementation as `os.path`.
        Returns: tree, trees, blobs
        """
        trees, blobs = [], []
        for tree_entry in tree:
            obj = repo.get(tree_entry.oid)
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

    def store_tree_from(repo, commit):
        """Walk the tree and send the encountered objects to api backend.
        """
        sha1s_map = swhmap.SWHMap()

        for ori_tree_ref, trees_ref, blobs_ref in \
                treewalk(repo, commit.tree):

            for blob_ref in blobs_ref:
                data = blob_ref.data
                blob_data_sha1hex = hash.hashkey_sha1(data).hexdigest()
                sha1s_map.add(store.Type.content, blob_ref, blob_data_sha1hex)

            for tree_ref in trees_ref:
                sha1s_map.add(store.Type.directory, tree_ref)

            sha1s_map.add(store.Type.directory, ori_tree_ref)

        sha1s_map.add(store.Type.revision, commit)

        return sha1s_map

    def store_commit(repo, commit_to_store):
        """Store a commit in swh storage.
        """
        sha1s_map = store_tree_from(repo, commit_to_store)
        sha1s_hex = sha1s_map.get_all_sha1s()
        unknown_ref_sha1s = client.post(baseurl, {'sha1s': sha1s_hex})

        client.put_all(baseurl, unknown_ref_sha1s, sha1s_map)

    def walk_revision_from(repo, head_revision, visited):
        """Walk the revision from commit head_revision.
        - repo is the current repository
        - head_revision is the latest commit to start from
        - visited is a memory cache of visited node (implemented as set)
        """
        to_visits = [head_revision]  # the nodes to visit topologically
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
            head_revision = repo[ref.target] \
                          if ref.type is GIT_REF_OID \
                          else ref.peel(GIT_OBJ_COMMIT)  # noqa
            walk_revision_from(repo, head_revision, visited)

    walk_references_from(pygit2.Repository(repo_path))


def parse(repo):
    """Given a repository path, parse and return a memory model of such
    repository."""
    sha1s_map = swhmap.SWHMap()

    sha1s_map.add_origin('git', repo.path)

    for ref_name in repo.listall_references():
        logging.info('walk reference %s' % ref_name)

        ref = repo.lookup_reference(ref_name)

        head_revision = repo[ref.target] \
                        if ref.type is GIT_REF_OID \
                        else ref.peel(GIT_OBJ_COMMIT)

        if isinstance(head_revision, pygit2.Tag):
            sha1s_map.add_release(head_revision.hex, ref_name)
        else:
            sha1s_map.add_occurrence(head_revision.hex, ref_name)

        # walk_revision_from(repo, head_revision, visited)


    return sha1s_map


def load_to_back(backend_url, sha1s_map):
    """Load to the backend_url the repository sha1s_map.
    """
    print(sha1s_map)


def load(conf):
    """According to action, load the repository.

    used configuration keys:
    - action: requested action
    - repository: git repository path ('load' action only)
    - backend_url: url access to backend api
   """
    action = conf['action']

    if action == 'load':
        logging.info('load repository %s' % conf['repository'])

        repo_path = conf['repository']
        if not os.path.exists(repo_path):
            logging.error('Repository %s does not exist.' % repo_path)
            raise Exception('Repository %s does not exist.' % repo_path)

        repo = pygit2.Repository(repo_path)
        sha1s_map = parse(repo)
        load_to_back(conf['backend_url'], sha1s_map)
    else:
        logging.warn('skip unknown-action %s' % action)
