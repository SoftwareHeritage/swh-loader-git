# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import pygit2
import os
import time

from pygit2 import GIT_REF_OID
from pygit2 import GIT_OBJ_COMMIT, GIT_OBJ_TREE

from swh import hash
from swh.storage import store
from swh.data import swhmap
from swh.http import client

def now():
    """Cheat time values."""
    return time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())


def parse(repo):
    """Given a repository path, parse and return a memory model of such
    repository."""
    def treewalk(repo, tree):
        """Walk a tree with the same implementation as `os.path`.
        Returns: tree, trees, blobs
        """
        trees, blobs, directory_entries = [], [], []
        for tree_entry in tree:
            obj = repo.get(tree_entry.oid)
            if obj is None:
                logging.warn('skip submodule-commit %s' % tree_entry.hex)
                continue  # submodule!

            if obj.type == GIT_OBJ_TREE:
                nature = 'directory'  # FIXME use enum
                trees.append(tree_entry)
            else:
                data = obj.data
                nature = 'file'  # FIXME use enum
                blobs.append({'sha1': obj.hex,
                              'content-sha1': hash.hash1(data).hexdigest(),
                              'content-sha256': hash.hash256(data).hexdigest(),
                              'content': data.decode('utf-8'),
                              'size': obj.size})

                directory_entries.append({'name': tree_entry.name,
                                          'target-sha1': obj.hex,
                                          'nature': nature,
                                          'perms': tree_entry.filemode,
                                          'atime': now(),  # FIXME use real data
                                          'mtime': now(),  # FIXME use real data
                                          'ctime': now(),  # FIXME use real data
                                          'parent': tree.hex})

        yield tree, directory_entries, trees, blobs
        for tree_entry in trees:
            for x in treewalk(repo, repo[tree_entry.oid]):
                yield x

    # FIXME check all commits are walked
    def walk_revision_from(repo, swhrepo, revision):
        """Walk the revision from revision.
        - repo is the current repository
        - revision is the latest revision to start from.
        """
        for directory_root, directory_entries, _, contents_ref in \
            treewalk(repo, revision.tree):
            for content_ref in contents_ref:
                swhrepo.add_content(content_ref)

            directory_content = str(directory_root.read_raw())
            # .decode('utf-8')
            # FIXME hack to avoid UnicodeDecodeError: 'utf-8' codec can't decode
            # byte 0x8b in position 17: invalid start byte
            swhrepo.add_directory({'sha1': directory_root.hex,
                                   'content': directory_content,
                                   'entries': directory_entries})

        swhrepo.add_revision({'sha1': revision.hex,
                              'content': revision.read_raw().decode('utf-8'),
                              'date': revision.commit_time,
                              'directory': revision.tree.hex,
                              'message': revision.message,
                              'committer': revision.committer.email,
                              'author': revision.author.email})

        return swhrepo

    # memory model
    swhrepo = swhmap.SWHRepo()
    # add origin
    swhrepo.add_origin('git', repo.path)
    # add references and crawl them
    for ref_name in repo.listall_references():
        logging.info('walk reference %s' % ref_name)
        ref = repo.lookup_reference(ref_name)

        head_revision = repo[ref.target] \
                        if ref.type is GIT_REF_OID \
                        else ref.peel(GIT_OBJ_COMMIT)

        if isinstance(head_revision, pygit2.Tag):
            swhrepo.add_release(head_revision.hex, ref_name)
            head_start = head_revision.get_object()
        else:
            swhrepo.add_occurrence(head_revision.hex, ref_name)
            head_start = head_revision

        # crawl commits and trees
        walk_revision_from(repo, swhrepo, head_start)

    return swhrepo


def load_to_back(backend_url, swhrepo):
    """Load to the backend_url the repository swhrepo.
    """
    ##### origins

    # first, store/retrieve the origin identifier
    origin_id = client.put(backend_url,
                           obj_type=store.Type.origin,
                           obj=swhrepo.get_origin(),
                           key_result='id')

    print("origin id: ", origin_id)

    ##### contents

    # have: filter contents
    unknown_content_sha1s = client.post(backend_url,
                                        store.Type.content,
                                        swhrepo.get_contents().keys(),
                                        key_result='sha1s')

    # seen: contents to store in backend
    contents_to_store = []
    contents_map = swhrepo.get_contents().objects()
    for unknown_ref in unknown_content_sha1s:
        contents_to_store.append(contents_map.get(unknown_ref))

    # store unknown contents
    client.put_all(backend_url, store.Type.content, contents_to_store)

    ##### directories

    # have: filter contents
    unknown_directory_sha1s = client.post(backend_url,
                                          store.Type.directory,
                                          swhrepo.get_directories().keys(),
                                          key_result='sha1s')
    print(unknown_directory_sha1s)

    # seen: contents to store in backend
    directories_to_store = []
    directories_map = swhrepo.get_directories().objects()
    for unknown_ref in unknown_directory_sha1s:
        obj = directories_map.get(unknown_ref)
        directories_to_store.append(obj)

    # store unknown directories
    client.put_all(backend_url, store.Type.directory, directories_to_store)

    # have filter directories...

    ##### directories

    # have: filter occurrences...

    ##### releases

    # have: filter releases...

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
        swhrepo = parse(repo)
        load_to_back(conf['backend_url'], swhrepo)
    else:
        logging.warn('skip unknown-action %s' % action)
