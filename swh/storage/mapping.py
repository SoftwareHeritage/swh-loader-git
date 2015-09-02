#!/usr/bin/env python3
# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime

from swh.storage import store


def build_content(sha1hex, obj_partial):
    """Build a content object from the obj_partial.
    """
    obj_partial = obj_partial if obj_partial else {}
    return {'id': sha1hex,
            'type': store.Type.content,
            'content-sha1': obj_partial.get('content-sha1'),
            'content-sha256': obj_partial.get('content-sha256'),
            'content': obj_partial.get('content'),
            'size': obj_partial.get('size')}


def build_directory(sha1hex, obj_partial):
    """Build a directory object from the obj_partial.
    """
    obj_partial = obj_partial if obj_partial else {}  # FIXME get hack -> split get-post/put
    directory = {'id': sha1hex,
                 'type': store.Type.directory,
                 'content': obj_partial.get('content')}

    directory_entries = []
    for entry in obj_partial.get('entries', []):
        directory_entry = build_directory_entry(sha1hex, entry)
        directory_entries.append(directory_entry)

    directory.update({'entries': directory_entries})
    return directory


def date_from_string(str_date):
    """Convert a string date with format '%a, %d %b %Y %H:%M:%S +0000'.
    """
    return datetime.strptime(str_date, '%a, %d %b %Y %H:%M:%S +0000')


def build_directory_entry(parent_sha1hex, entry):
    """Build a directory object from the entry.
    """
    return {'name': entry['name'],
            'target-sha1': entry['target-sha1'],
            'nature': entry['nature'],
            'perms': entry['perms'],
            'atime': date_from_string(entry['atime']),
            'mtime': date_from_string(entry['mtime']),
            'ctime': date_from_string(entry['ctime']),
            'parent': entry['parent']}


def build_revision(sha1hex, obj_partial):
    """Build a revision object from the obj_partial.
    """
    obj = {'id': sha1hex,
           'type': store.Type.revision}
    if obj_partial:
        obj.update({'content': obj_partial['content'],
                    'date': date_from_string(obj_partial['date']),
                    'directory': obj_partial['directory'],
                    'message': obj_partial['message'],
                    'author': obj_partial['author'],
                    'committer': obj_partial['committer'],
                    'parent-sha1s': obj_partial['parent-sha1s']})
    return obj


def build_release(sha1hex, obj_partial):
    """Build a release object from the obj_partial.
    """
    obj = {'id': sha1hex,
           'type': store.Type.release}
    if obj_partial:
        obj.update({'id': sha1hex,
                    'content': obj_partial['content'],
                    'revision': obj_partial['revision'],
                    'date': obj_partial['date'],
                    'name': obj_partial['name'],
                    'comment': obj_partial['comment'],
                    'author': obj_partial['author']})
    return obj


def build_occurrence(sha1hex, obj_partial):
    """Build a content object from the obj_partial.
    """
    obj = {'id': sha1hex,
           'type': store.Type.occurrence}
    if obj_partial:
        obj.update({'reference': obj_partial['reference'],
                    'type': store.Type.occurrence,
                    'revision': sha1hex,
                    'url-origin': obj_partial['url-origin']})
    return obj


def build_origin(sha1hex, obj_partial):
    """Build an origin.
    """
    obj = {'id': obj_partial['url'],
           'origin-type': obj_partial['type']}
    return obj
