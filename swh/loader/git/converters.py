# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert pygit2 objects to dictionaries suitable for swh.storage"""

from pygit2 import GIT_OBJ_COMMIT

from swh.core import hashutil

from .utils import format_date

HASH_ALGORITHMS = ['sha1', 'sha256']


def blob_to_content(id, repo, log=None, max_content_size=None, origin_id=None):
    """Format a blob as a content"""
    blob = repo[id]
    size = blob.size
    ret = {
        'sha1_git': id.raw,
        'length': blob.size,
        'status': 'absent'
    }

    if max_content_size:
        if size > max_content_size:
            if log:
                log.info('Skipping content %s, too large (%s > %s)' %
                         (id.hex, size, max_content_size), extra={
                             'swh_type': 'loader_git_content_skip',
                             'swh_repo': repo.path,
                             'swh_id': id.hex,
                             'swh_size': size,
                         })
            ret['reason'] = 'Content too large'
            ret['origin'] = origin_id
            return ret

    data = blob.data
    hashes = hashutil.hashdata(data, HASH_ALGORITHMS)
    ret.update(hashes)
    ret['data'] = data
    ret['status'] = 'visible'

    return ret


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
            'name': entry._name,
            'target': entry.id.raw,
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
        'committer_date': format_date(committer),
        'type': 'git',
        'directory': commit.tree_id.raw,
        'message': commit.raw_message,
        'metadata': None,
        'author': {
            'name': author.raw_name,
            'email': author.raw_email,
        },
        'committer': {
            'name': committer.raw_name,
            'email': committer.raw_email,
        },
        'synthetic': False,
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
                tag_pointer.id.hex), extra={
                    'swh_type': 'loader_git_tag_ignore',
                    'swh_repo': repo.path,
                    'swh_tag_id': tag.id.hex,
                    'swh_tag_dest': {
                        'type': tag_pointer.__class__.__name__,
                        'id': tag_pointer.id.hex,
                    },
                })
        return

    if not tag.tagger:
        if log:
            log.warn("Tag %s has no author, using default values"
                     % id.hex,  extra={
                         'swh_type': 'loader_git_tag_author_default',
                         'swh_repo': repo.path,
                         'swh_tag_id': tag.id.hex,
                     })
        author = None
        date = None
    else:
        author = {
            'name': tag.tagger.raw_name,
            'email': tag.tagger.raw_email,
        }
        date = format_date(tag.tagger)

    return {
        'id': id.raw,
        'date': date,
        'target': tag.target.raw,
        'target_type': 'revision',
        'message': tag._message,
        'name': tag.name.raw,
        'author': author,
        'metadata': None,
        'synthetic': False,
    }


def ref_to_occurrence(ref):
    """Format a reference as an occurrence"""
    occ = ref.copy()
    if 'branch' in ref:
        branch = ref['branch']
        if isinstance(branch, str):
            occ['branch'] = branch.encode('utf-8')
        else:
            occ['branch'] = branch
    return occ


def origin_url_to_origin(origin_url):
    """Format a pygit2.Repository as an origin suitable for swh.storage"""
    return {
        'type': 'git',
        'url': origin_url,
    }


def dulwich_blob_to_content(blob, log=None, max_content_size=None,
                            origin_id=None):
    """Convert a dulwich blob to a Software Heritage content"""

    size = blob.raw_length()

    ret = {
        'sha1_git': blob.sha().digest(),
        'length': size,
        'status': 'absent'
    }

    if max_content_size:
        if size > max_content_size:
            if log:
                log.info('Skipping content %s, too large (%s > %s)' %
                         (blob.id.encode(), size, max_content_size), extra={
                             'swh_type': 'loader_git_content_skip',
                             'swh_id': id.hex,
                             'swh_size': size,
                         })
            ret['reason'] = 'Content too large'
            ret['origin'] = origin_id
            return ret

    data = blob.as_raw_string()
    hashes = hashutil.hashdata(data, HASH_ALGORITHMS)
    ret.update(hashes)
    ret['data'] = data
    ret['status'] = 'visible'

    return ret


def dulwich_tree_to_directory(tree, log=None):
    """Format a tree as a directory"""
    ret = {
        'id': tree.sha().digest(),
    }
    entries = []
    ret['entries'] = entries

    entry_mode_map = {
        0o040000: 'dir',
        0o160000: 'rev',
        0o100644: 'file',
        0o100755: 'file',
        0o120000: 'file',
    }

    for entry in tree.iteritems():
        entries.append({
            'type': entry_mode_map[entry.mode],
            'perms': entry.mode,
            'name': entry.path,
            'target': hashutil.hex_to_hash(entry.sha.decode('ascii')),
        })

    return ret


def dulwich_commit_to_revision(commit, log=None):
    pass


DULWICH_CONVERTERS = {
    b'blob': dulwich_blob_to_content,
    b'tree': dulwich_tree_to_directory,
    b'commit': lambda x: x,
    b'tag': lambda x: x,
}
