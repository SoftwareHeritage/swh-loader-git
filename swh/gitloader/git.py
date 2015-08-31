# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import pygit2
import time

from datetime import datetime
from pygit2 import GIT_REF_OID
from pygit2 import GIT_OBJ_COMMIT, GIT_OBJ_TREE, GIT_SORT_TOPOLOGICAL
from enum import Enum

from swh import hash
from swh.data import swhmap


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
            obj = repo.get(tree_entry.oid)
            if obj is None:
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
                blobs.append({'sha1': obj.hex,
                              'content-sha1': hash.hash1(data).hexdigest(),
                              'content-sha256': hash.hash256(data).hexdigest(),
                              'content': data,  # FIXME: add pointer to data on disk?
                              'size': obj.size})

            logging.debug('(name: %s, tgt: %s, nat: %s, perms: %s, parent: %s) ' %  # noqa
                          (tree_entry.name,
                           obj.hex, nature,
                           tree_entry.filemode,
                           tree.hex))

            dir_entries.append({'name': tree_entry.name,
                                'target-sha1': obj.hex,
                                'nature': nature,
                                'perms': tree_entry.filemode,
                                'atime': now(),  # FIXME: use real data
                                'mtime': now(),  # FIXME: use real data
                                'ctime': now(),  # FIXME: use real data
                                'parent': tree.hex})

        yield tree, dir_entries, trees, blobs
        for tree_entry in trees:
            for x in treewalk(repo, repo[tree_entry.oid]):
                yield x

    def walk_tree(repo, swhrepo, rev):
        """Walk the rev revision's directories.
        """
        if swhrepo.already_visited(rev.hex):
            logging.debug('commit %s already visited, skipped' % rev.hex)
            return swhrepo

        for dir_root, dir_entries, _, contents_ref in treewalk(repo, rev.tree):
            for content_ref in contents_ref:
                swhrepo.add_content(content_ref)

            swhrepo.add_directory({'sha1': dir_root.hex,
                                   'content': dir_root.read_raw(),  # FIXME: add pointer to data on disk?
                                   'entries': dir_entries})

        revision_parent_sha1s = list(map(str, rev.parent_ids))
        swhrepo.add_revision({'sha1': rev.hex,
                              'content': rev.read_raw(),  # FIXME: add pointer to data on disk?
                              'date': timestamp_to_string(rev.commit_time),
                              'directory': rev.tree.hex,
                              'message': rev.message,
                              'committer': read_signature(rev.committer),
                              'author': read_signature(rev.author),
                              'parent-sha1s': revision_parent_sha1s
                              })

        return swhrepo

    def walk_revision_from(repo, swhrepo, head_revision):
        """Walk the rev history log from head_revision.
        - repo is the current repository
        - rev is the latest rev to start from.
        """
        for rev in repo.walk(head_revision.id, GIT_SORT_TOPOLOGICAL):
            swhrepo = walk_tree(repo, swhrepo, rev)

        return swhrepo

    repo = pygit2.Repository(repo_path)
    # memory model
    swhrepo = swhmap.SWHRepo()
    # add origin
    origin = {'type': 'git',
              'url': 'file://' + repo.path}
    swhrepo.add_origin(origin)
    # add references and crawl them
    for ref_name in repo.listall_references():
        logging.info('walk reference %s' % ref_name)
        ref = repo.lookup_reference(ref_name)

        head_revision = repo[ref.target] \
                        if ref.type is GIT_REF_OID \
                        else ref.peel(GIT_OBJ_COMMIT)

        if isinstance(head_revision, pygit2.Tag):
            head_start = head_revision.get_object()
            release = {'sha1': head_revision.hex,
                       'content': head_revision.read_raw(),  # FIXME: add pointer to data on disk?
                       'revision': head_revision.target.hex,
                       'name': ref_name,
                       'date': now(),  # FIXME: find the tag's date,
                       'author':  read_signature(head_revision.tagger),
                       'comment': head_revision.message}
            swhrepo.add_release(release)
        else:
            swhrepo.add_occurrence({'sha1': head_revision.hex,
                                    'reference': ref_name,
                                    'url-origin': origin['url']})
            head_start = head_revision

        # crawl commits and trees
        walk_revision_from(repo, swhrepo, head_start)

    return swhrepo
