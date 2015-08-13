#!/usr/bin/env python3
# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime

from swh.storage import store


def build_content(sha1hex, payload):
    """Build a content object from the payload.
    """
    payload = payload if payload else {}
    return {'sha1': sha1hex,
            'type': store.Type.content,
            'content-sha1': payload.get('content-sha1'),
            'content-sha256': payload.get('content-sha256'),
            'content': payload.get('content'),
            'size': payload.get('size')}


def build_directory(sha1hex, payload):
    """Build a directory object from the payload.
    """
    payload = payload if payload else {}  # FIXME get hack -> split get-post/put
    directory = {'sha1': sha1hex,
                 'type': store.Type.directory,
                 'content': payload.get('content')}

    directory_entries = []
    for entry in payload.get('entries', []):
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



def build_revision(sha1hex, payload):
    """Build a revision object from the payload.
    """
    obj = {'sha1': sha1hex,
           'type': store.Type.revision}
    if payload:
        obj.update({'content': payload['content'],
                    'date': date_from_string(payload['date']),
                    'directory': payload['directory'],
                    'message': payload['message'],
                    'author': payload['author'],
                    'committer': payload['committer'],
                    'parent-sha1s': payload['parent-sha1s']})
    return obj


def build_release(sha1hex, payload):
    """Build a release object from the payload.
    """
    obj = {'sha1': sha1hex,
           'type': store.Type.release}
    if payload:
        obj.update({'sha1': sha1hex,
                    'content': payload['content'],
                    'revision': payload['revision'],
                    'date': payload['date'],
                    'name': payload['name'],
                    'comment': payload['comment'],
                    'author': payload['author']})
    return obj


def build_occurrence(sha1hex, payload):
    """Build a content object from the payload.
    """
    obj = {'sha1': sha1hex,
           'type': store.Type.occurrence}
    if payload:
        obj.update({'reference': payload['reference'],
                    'type': store.Type.occurrence,
                    'revision': sha1hex,
                    'url-origin': payload['url-origin']})
    return obj


def build_origin(sha1hex, payload):
    """Build an origin.
    """
    obj = {'id': payload['url'],
           'origin-type': payload['type']}
    return obj
