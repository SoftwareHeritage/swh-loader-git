# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
import logging
import os
import subprocess
import time

import pygit2

from datetime import datetime
from pygit2 import GIT_REF_OID
from pygit2 import GIT_OBJ_COMMIT, GIT_OBJ_TREE, GIT_SORT_TOPOLOGICAL
from enum import Enum

from swh.core import hashutil
from swh.loader.git.data import swhrepo
from swh.loader.git.store import store


class DirectoryTypeEntry(Enum):
    """Types of git objects.
    """
    file = 'file'
    directory = 'directory'


def date_format(d):
    """d is expected to be a datetime object.
    """
    return time.strftime("%a, %d %b %Y %H:%M:%S +0000", d.timetuple())


def now():
    """Cheat time values."""
    return date_format(datetime.utcnow())


def timestamp_to_string(timestamp):
    """Convert a timestamps to string.
    """
    return date_format(datetime.utcfromtimestamp(timestamp))

def list_objects_from_packfile_index(packfile_index):
    """List the objects indexed by this packfile"""
    input_file = open(packfile_index, 'rb')
    with subprocess.Popen(
        ['/usr/bin/git', 'show-index'],
        stdin=input_file,
        stdout=subprocess.PIPE,
    ) as process:
        for line in process.stdout.readlines():
            obj_id = line.decode('utf-8', 'ignore').split()[1]
            yield obj_id

def list_objects(repo):
    """List the objects in a given repository"""
    objects_dir = os.path.join(repo.path, 'objects')
    objects_glob = os.path.join(objects_dir, '[0-9a-f]' * 2, '[0-9a-f]' * 38)

    packfile_dir = os.path.join(objects_dir, 'pack')

    if os.path.isdir(packfile_dir):
        for packfile_index in os.listdir(packfile_dir):
            if not packfile_index.endswith('.idx'):
                # Not an index file
                continue
            packfile_index_path = os.path.join(packfile_dir, packfile_index)
            yield from list_objects_from_packfile_index(packfile_index_path)

    for object_file in glob.glob(objects_glob):
        yield ''.join(object_file.split(os.path.sep)[-2:])


HASH_ALGORITHMS=['sha1', 'sha256']


def parse(repo_path):
    """Given a repository path, parse and return a memory model of such
    repository."""
    def read_signature(signature):
        return '%s <%s>' % (signature.name, signature.email)

    def treewalk(repo, tree):
        """Walk a tree with the same implementation as `os.path`.
        Returns: tree, trees, blobs
        """
        trees, blobs, dir_entries = [], [], []
        for tree_entry in tree:
            if swh_repo.already_visited(tree_entry.hex):
                logging.debug('tree_entry %s already visited, skipped' % tree_entry.hex)
                continue

            obj = repo.get(tree_entry.oid)
            if obj is None:  # or obj.type == GIT_OBJ_COMMIT:
                logging.warn('skip submodule-commit %s' % tree_entry.hex)
                continue  # submodule!

            if obj.type == GIT_OBJ_TREE:
                logging.debug('found tree %s' % tree_entry.hex)
                nature = DirectoryTypeEntry.directory.value
                trees.append(tree_entry)
            else:
                logging.debug('found content %s' % tree_entry.hex)
                data = obj.data
                nature = DirectoryTypeEntry.file.value
                hashes = hashutil.hashdata(data, HASH_ALGORITHMS)
                blobs.append({'id': obj.hex,
                              'type': store.Type.content,
                              'content-sha1': hashes['sha1'],
                              'content-sha256': hashes['sha256'],
                              'content': data,
                              'size': obj.size})

            dir_entries.append({'name': tree_entry.name,
                                'type': store.Type.directory_entry,
                                'target-sha1': obj.hex,
                                'nature': nature,
                                'perms': tree_entry.filemode,
                                'atime': None,
                                'mtime': None,
                                'ctime': None,
                                'parent': tree.hex})

        yield tree, dir_entries, trees, blobs
        for tree_entry in trees:
            for x in treewalk(repo, repo[tree_entry.oid]):
                yield x

    def walk_tree(repo, swh_repo, rev):
        """Walk the rev revision's directories.
        """
        if swh_repo.already_visited(rev.hex):
            logging.debug('commit %s already visited, skipped' % rev.hex)
            return swh_repo

        for dir_root, dir_entries, _, contents_ref in treewalk(repo, rev.tree):
            for content_ref in contents_ref:
                swh_repo.add_content(content_ref)

            swh_repo.add_directory({'id': dir_root.hex,
                                    'type': store.Type.directory,
                                    'entries': dir_entries})

        revision_parent_sha1s = list(map(str, rev.parent_ids))

        author = {'name': rev.author.name,
                  'email': rev.author.email,
                  'type': store.Type.person}
        committer = {'name': rev.committer.name,
                     'email': rev.committer.email,
                     'type': store.Type.person}

        swh_repo.add_revision({'id': rev.hex,
                               'type':store.Type.revision,
                               'date': timestamp_to_string(rev.commit_time),
                               'directory': rev.tree.hex,
                               'message': rev.message,
                               'committer': committer,
                               'author': author,
                               'parent-sha1s': revision_parent_sha1s
        })

        swh_repo.add_person(read_signature(rev.author), author)
        swh_repo.add_person(read_signature(rev.committer), committer)

        return swh_repo

    def walk_revision_from(repo, swh_repo, head_rev):
        """Walk the rev history log from head_rev.
        - repo is the current repository
        - rev is the latest rev to start from.
        """
        for rev in repo.walk(head_rev.id, GIT_SORT_TOPOLOGICAL):
            swh_repo = walk_tree(repo, swh_repo, rev)

        return swh_repo

    repo = pygit2.Repository(repo_path)
    # memory model
    swh_repo = swhrepo.SWHRepo()
    # add origin
    origin = {'type': 'git',
              'url': 'file://' + repo.path}
    swh_repo.add_origin(origin)
    # add references and crawl them
    for ref_name in repo.listall_references():
        logging.info('walk reference %s' % ref_name)
        ref = repo.lookup_reference(ref_name)

        head_rev = repo[ref.target] \
                        if ref.type is GIT_REF_OID \
                        else ref.peel(GIT_OBJ_COMMIT)  # noqa

        if isinstance(head_rev, pygit2.Tag):
            head_start = head_rev.get_object()
            taggerSig = head_rev.tagger
            author = {'name': taggerSig.name,
                      'email': taggerSig.email,
                      'type': store.Type.person}
            release = {'id': head_rev.hex,
                       'type': store.Type.release,
                       'revision': head_rev.target.hex,
                       'name': ref_name,
                       'date': now(),  # FIXME: find the tag's date,
                       'author':  author,
                       'comment': head_rev.message}

            swh_repo.add_release(release)
            swh_repo.add_person(read_signature(taggerSig), author)
        else:
            swh_repo.add_occurrence({'id': head_rev.hex,
                                     'revision': head_rev.hex,
                                     'reference': ref_name,
                                     'url-origin': origin['url'],
                                     'type': store.Type.occurrence})
            head_start = head_rev

        # crawl commits and trees
        walk_revision_from(repo, swh_repo, head_start)

    return swh_repo
