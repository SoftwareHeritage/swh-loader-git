# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert objects to dictionaries suitable for swh.storage"""

import os

from swh.model.hashutil import hash_to_hex

from swh.model import git


def blob_to_content(obj, log=None, max_content_size=None,
                    origin_id=None):
    """Convert obj to a swh storage content.

    Note:
    - If obj represents a link, the length and data are already
    provided so we use them directly.
    - 'data' is returned only if max_content_size is not reached.

    Returns:
        obj converted to content as a dictionary.

    """
    filepath = obj['path']
    if 'length' in obj:  # link already has it
        size = obj['length']
    else:
        size = os.lstat(filepath).st_size

    ret = {
        'sha1': obj['sha1'],
        'sha256': obj['sha256'],
        'sha1_git': obj['sha1_git'],
        'length': size,
        'perms': obj['perms'].value,
        'type': obj['type'].value,
    }

    if max_content_size and size > max_content_size:
        if log:
            log.info('Skipping content %s, too large (%s > %s)' %
                     (hash_to_hex(obj['sha1_git']),
                      size,
                      max_content_size))
        ret.update({'status': 'absent',
                    'reason': 'Content too large',
                    'origin': origin_id})
        return ret

    if 'data' in obj:  # link already has it
        data = obj['data']
    else:
        data = open(filepath, 'rb').read()

    ret.update({
        'data': data,
        'status': 'visible'
    })

    return ret


# Map of type to swh types
_entry_type_map = {
    git.GitType.TREE: 'dir',
    git.GitType.BLOB: 'file',
    git.GitType.COMM: 'rev',
}


def tree_to_directory(tree, log=None):
    """Format a tree as a directory

    """
    entries = []
    for entry in tree['children']:
        entries.append({
            'type': _entry_type_map[entry['type']],
            'perms': int(entry['perms'].value),
            'name': entry['name'],
            'target': entry['sha1_git']
        })

    return {
        'id': tree['sha1_git'],
        'entries': entries
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


def shallow_blob(obj):
    """Convert a full swh content/blob to just what's needed by
    swh-storage for filtering.

    Returns:
        A shallow copy of a full swh content/blob object.

    """
    return {
        'sha1': obj['sha1'],
        'sha256': obj['sha256'],
        'sha1_git': obj['sha1_git'],
        'length': obj['length']
    }


def shallow_tree(tree):
    """Convert a full swh directory/tree to just what's needed by
    swh-storage for filtering.

    Returns:
        A shallow copy of a full swh directory/tree object.

    """
    return tree['sha1_git']


def shallow_commit(commit):
    """Convert a full swh revision/commit to just what's needed by
    swh-storage for filtering.

    Returns:
        A shallow copy of a full swh revision/commit object.

    """
    return commit['id']


def shallow_tag(tag):
    """Convert a full swh release/tag to just what's needed by
    swh-storage for filtering.

    Returns:
        A shallow copy of a full swh release/tag object.

    """
    return tag['id']
