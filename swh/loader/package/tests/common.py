# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import path
import logging
from urllib.parse import urlparse

from swh.model.hashutil import hash_to_bytes, hash_to_hex


logger = logging.getLogger(__file__)

DATADIR = path.join(path.abspath(path.dirname(__file__)), 'resources')


def get_response_cb(request, context):
    """Mount point callback to fetch on disk the content of a request

    This is meant to be used as 'body' argument of the requests_mock.get()
    method.

    It will look for files on the local filesystem based on the requested URL,
    using the following rules:

    - files are searched in the DATADIR/<hostname> directory

    - the local file name is the path part of the URL with path hierarchy
      markers (aka '/') replaced by '_'

    Eg. if you use the requests_mock fixture in your test file as:

        requests_mock.get('https://nowhere.com', body=get_response_cb)
        # or even
        requests_mock.get(re.compile('https://'), body=get_response_cb)

    then a call requests.get like:

        requests.get('https://nowhere.com/path/to/resource')

    will look the content of the response in:

        DATADIR/resources/nowhere.com/path_to_resource

    Args:
        request (requests.Request): Object requests
        context (requests.Context): Object holding response metadata
                                    informations (status_code, headers, etc...)

    Returns:
        File descriptor on the on disk file to read from the test context

    """
    logger.debug('get_response_cb(%s, %s)', request, context)
    url = urlparse(request.url)
    dirname = url.hostname  # pypi.org | files.pythonhosted.org
    # url.path: pypi/<project>/json -> local file: pypi_<project>_json
    filename = url.path[1:]
    if filename.endswith('/'):
        filename = filename[:-1]
    filename = filename.replace('/', '_')
    filepath = path.join(DATADIR, dirname, filename)
    if not path.isfile(filepath):
        context.status_code = 404
        return None
    fd = open(filepath, 'rb')
    context.headers['content-length'] = str(path.getsize(filepath))
    return fd


def decode_target(target):
    if not target:
        return target
    target_type = target['target_type']

    if target_type == 'alias':
        decoded_target = target['target'].decode('utf-8')
    else:
        decoded_target = hash_to_hex(target['target'])

    return {
        'target': decoded_target,
        'target_type': target_type
    }


def check_snapshot(expected_snapshot, expected_branches, storage):
    """Check for snapshot match.

    Provide the hashes as hexadecimal, the conversion is done
    within the method.

    Args:
        expected_snapshot (Union[str, dict]): Either the snapshot
                                      identifier or the full
                                      snapshot
        expected_branches ([dict]): expected branches or nothing is
                                  the full snapshot is provided

    """
    if isinstance(expected_snapshot, dict) and not expected_branches:
        expected_snapshot_id = expected_snapshot['id']
        expected_branches = expected_snapshot['branches']
    else:
        expected_snapshot_id = expected_snapshot

    snap = storage.snapshot_get(hash_to_bytes(expected_snapshot_id))
    assert snap is not None

    branches = {
        branch.decode('utf-8'): decode_target(target)
        for branch, target in snap['branches'].items()
    }
    assert expected_branches == branches
