# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert objects to dictionaries suitable for swh.storage"""

from swh.model.hashutil import hash_to_hex


def content_for_storage(content, log=None, max_content_size=None,
                        origin_id=None):
    """Prepare content to be ready for storage

    Note:
    - 'data' is returned only if max_content_size is not reached.

    Returns:
        content with added data (or reason for being missing)

    """
    ret = content.copy()

    if max_content_size and ret['length'] > max_content_size:
        if log:
            log.info('Skipping content %s, too large (%s > %s)' %
                     (hash_to_hex(content['sha1_git']),
                      ret['length'],
                      max_content_size))
        ret.pop('data', None)
        ret.update({'status': 'absent',
                    'reason': 'Content too large',
                    'origin': origin_id})
        return ret

    if 'data' not in ret:
        ret['data'] = open(ret['path'], 'rb').read()

    ret['status'] = 'visible'

    return ret
