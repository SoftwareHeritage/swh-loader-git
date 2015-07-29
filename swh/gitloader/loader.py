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

def parse(repo):
    """Given a repository path, parse and return a memory model of such
    repository."""
    def treewalk(repo, tree, topdown=False):
        """Walk a tree with the same implementation as `os.path`.
        Returns: tree, trees, blobs
        """
        trees, blobs = [], []
        for tree_entry in tree:
            obj = repo.get(tree_entry.oid)
            if obj is None:
                logging.warn('skip submodule-head_revision %s' % tree_entry.hex)
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

    def walk_revision_from(repo, sha1s_map, head_revision):
        """Walk the revision from head_revision.
        - repo is the current repository
        - head_revision is the latest revision to start from.
        """
        for ori_tree_ref, trees_ref, blobs_ref in \
                treewalk(repo, head_revision.tree):

             for blob_ref in blobs_ref:
                data = blob_ref.data
                blob_data_sha1hex = hash.hashkey_sha1(data).hexdigest()
                sha1s_map.add(store.Type.content, blob_ref, blob_data_sha1hex)

             for tree_ref in trees_ref:
                sha1s_map.add(store.Type.directory, tree_ref)

             sha1s_map.add(store.Type.directory, ori_tree_ref)

        sha1s_map.add(store.Type.revision, head_revision)

        return sha1s_map

    # memory model
    sha1s_map = swhmap.SWHMap()
    # add origin
    sha1s_map.add_origin('git', repo.path)
    # add references and crawl them
    for ref_name in repo.listall_references():
        logging.info('walk reference %s' % ref_name)

        ref = repo.lookup_reference(ref_name)

        head_revision = repo[ref.target] \
                        if ref.type is GIT_REF_OID \
                        else ref.peel(GIT_OBJ_COMMIT)

        if isinstance(head_revision, pygit2.Tag):
            sha1s_map.add_release(head_revision.hex, ref_name)
            head_start = head_revision.get_object()
        else:
            sha1s_map.add_occurrence(head_revision.hex, ref_name)
            head_start = head_revision

        # crawl commits and trees
        walk_revision_from(repo, sha1s_map, head_start)

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
