# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert dulwich objects to dictionaries suitable for swh.storage"""

from swh.model import identifiers
from swh.model.hashutil import (
    DEFAULT_ALGORITHMS, hash_to_hex, hash_to_bytes, MultiHash
)


HASH_ALGORITHMS = DEFAULT_ALGORITHMS - {'sha1_git'}


def origin_url_to_origin(origin_url):
    """Format a pygit2.Repository as an origin suitable for swh.storage"""
    return {
        'type': 'git',
        'url': origin_url,
    }


def dulwich_blob_to_content_id(blob):
    """Convert a dulwich blob to a Software Heritage content id"""
    if blob.type_name != b'blob':
        return

    size = blob.raw_length()
    data = blob.as_raw_string()
    hashes = MultiHash.from_data(data, HASH_ALGORITHMS).digest()
    hashes['sha1_git'] = blob.sha().digest()
    hashes['length'] = size
    return hashes


def dulwich_blob_to_content(blob, log=None, max_content_size=None,
                            origin_id=None):
    """Convert a dulwich blob to a Software Heritage content"""

    if blob.type_name != b'blob':
        return

    ret = dulwich_blob_to_content_id(blob)

    size = ret['length']

    if max_content_size:
        if size > max_content_size:
            id = hash_to_hex(ret['sha1_git'])
            if log:
                log.info('Skipping content %s, too large (%s > %s)' %
                         (id, size, max_content_size), extra={
                             'swh_type': 'loader_git_content_skip',
                             'swh_id': id,
                             'swh_size': size,
                         })
            ret['status'] = 'absent'
            ret['reason'] = 'Content too large'
            ret['origin'] = origin_id
            return ret

    data = blob.as_raw_string()
    ret['data'] = data
    ret['status'] = 'visible'

    return ret


def dulwich_tree_to_directory(tree, log=None):
    """Format a tree as a directory"""
    if tree.type_name != b'tree':
        return

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
            'type': entry_mode_map.get(entry.mode, 'file'),
            'perms': entry.mode,
            'name': entry.path,
            'target': hash_to_bytes(entry.sha.decode('ascii')),
        })

    return ret


def parse_author(name_email):
    """Parse an author line"""

    if name_email is None:
        return None

    try:
        open_bracket = name_email.index(b'<')
    except ValueError:
        name = email = None
    else:
        raw_name = name_email[:open_bracket]
        raw_email = name_email[open_bracket+1:]

        if not raw_name:
            name = None
        elif raw_name.endswith(b' '):
            name = raw_name[:-1]
        else:
            name = raw_name

        try:
            close_bracket = raw_email.index(b'>')
        except ValueError:
            email = None
        else:
            email = raw_email[:close_bracket]

    return {
        'name': name,
        'email': email,
        'fullname': name_email,
    }


def dulwich_tsinfo_to_timestamp(timestamp, timezone, timezone_neg_utc):
    """Convert the dulwich timestamp information to a structure compatible with
    Software Heritage"""
    return {
        'timestamp': timestamp,
        'offset': timezone // 60,
        'negative_utc': timezone_neg_utc if timezone == 0 else None,
    }


def dulwich_commit_to_revision(commit, log=None):
    if commit.type_name != b'commit':
        return

    ret = {
        'id': commit.sha().digest(),
        'author': parse_author(commit.author),
        'date': dulwich_tsinfo_to_timestamp(
            commit.author_time,
            commit.author_timezone,
            commit._author_timezone_neg_utc,
        ),
        'committer': parse_author(commit.committer),
        'committer_date': dulwich_tsinfo_to_timestamp(
            commit.commit_time,
            commit.commit_timezone,
            commit._commit_timezone_neg_utc,
        ),
        'type': 'git',
        'directory': bytes.fromhex(commit.tree.decode()),
        'message': commit.message,
        'metadata': None,
        'synthetic': False,
        'parents': [bytes.fromhex(p.decode()) for p in commit.parents],
    }

    git_metadata = []
    if commit.encoding is not None:
        git_metadata.append(['encoding', commit.encoding])
    if commit.mergetag:
        for mergetag in commit.mergetag:
            raw_string = mergetag.as_raw_string()
            assert raw_string.endswith(b'\n')
            git_metadata.append(['mergetag', raw_string[:-1]])

    if commit.extra:
        git_metadata.extend([k.decode('utf-8'), v] for k, v in commit.extra)

    if commit.gpgsig:
        git_metadata.append(['gpgsig', commit.gpgsig])

    if git_metadata:
        ret['metadata'] = {
            'extra_headers': git_metadata,
        }

    return ret


DULWICH_TYPES = {
    b'blob': 'content',
    b'tree': 'directory',
    b'commit': 'revision',
    b'tag': 'release',
}


def dulwich_tag_to_release(tag, log=None):
    if tag.type_name != b'tag':
        return

    target_type, target = tag.object
    ret = {
        'id': tag.sha().digest(),
        'name': tag.name,
        'target': bytes.fromhex(target.decode()),
        'target_type': DULWICH_TYPES[target_type.type_name],
        'message': tag._message,
        'metadata': None,
        'synthetic': False,
    }
    if tag.tagger:
        ret['author'] = parse_author(tag.tagger)
        if not tag.tag_time:
            ret['date'] = None
        else:
            ret['date'] = dulwich_tsinfo_to_timestamp(
                tag.tag_time,
                tag.tag_timezone,
                tag._tag_timezone_neg_utc,
            )
    else:
        ret['author'] = ret['date'] = None

    return ret


def branches_to_snapshot(branches):
    snapshot = {'branches': branches}
    snapshot_id = identifiers.snapshot_identifier(snapshot)
    snapshot['id'] = identifiers.identifier_to_bytes(snapshot_id)

    return snapshot
