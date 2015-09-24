# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert pygit2 objects to dictionaries suitable for swh.storage"""

from pygit2 import GIT_OBJ_COMMIT

from swh.core import hashutil

from .utils import format_date

HASH_ALGORITHMS = ['sha1', 'sha256']


def blob_to_content(id, repo, log=None):
    """Format a blob as a content"""
    blob = repo[id]
    data = blob.data
    hashes = hashutil.hashdata(data, HASH_ALGORITHMS)
    return {
        'sha1_git': id.raw,
        'sha1': hashes['sha1'],
        'sha256': hashes['sha256'],
        'data': data,
        'length': blob.size,
    }


def tree_to_directory(id, repo, log=None):
    """Format a tree as a directory"""
    ret = {
        'id': id.raw,
    }
    entries = []
    ret['entries'] = entries

    entry_type_map = {
        'tree': 'dir',
        'blob': 'file',
        'commit': 'rev',
    }

    for entry in repo[id]:
        entries.append({
            'type': entry_type_map[entry.type],
            'perms': entry.filemode,
            'name': entry.name,
            'target': entry.id.raw,
            'atime': None,
            'mtime': None,
            'ctime': None,
        })

    return ret


def commit_to_revision(id, repo, log=None):
    """Format a commit as a revision"""
    commit = repo[id]

    author = commit.author
    committer = commit.committer
    return {
        'id': id.raw,
        'date': format_date(author),
        'date_offset': author.offset,
        'committer_date': format_date(committer),
        'committer_date_offset': committer.offset,
        'type': 'git',
        'directory': commit.tree_id.raw,
        'message': commit.raw_message,
        'author_name': author.name,
        'author_email': author.email,
        'committer_name': committer.name,
        'committer_email': committer.email,
        'parents': [p.raw for p in commit.parent_ids],
    }


def annotated_tag_to_release(id, repo, log=None):
    """Format an annotated tag as a release"""
    tag = repo[id]

    tag_pointer = repo[tag.target]
    if tag_pointer.type != GIT_OBJ_COMMIT:
        if log:
            log.warn("Ignoring tag %s pointing at %s %s" % (
                tag.id.hex, tag_pointer.__class__.__name__,
                tag_pointer.id.hex))
        return

    author = tag.tagger

    if not author:
        if log:
            log.warn("Tag %s has no author, using default values"
                     % id.hex)
        author_name = ''
        author_email = ''
        date = None
        date_offset = 0
    else:
        author_name = author.name
        author_email = author.email
        date = format_date(author)
        date_offset = author.offset

    return {
        'id': id.raw,
        'date': date,
        'date_offset': date_offset,
        'revision': tag.target.raw,
        'comment': tag.message.encode('utf-8'),
        'author_name': author_name,
        'author_email': author_email,
    }


def ref_to_occurrence(ref):
    """Format a reference as an occurrence"""
    return ref


def repo_to_origin(repo):
    """Format a pygit2.Repository as an origin suitable for swh.storage"""
    return {
        'type': 'git',
        'url': 'file://%s' % repo.path,
    }
