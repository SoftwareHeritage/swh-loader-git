# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import logging
import os
import requests

from typing import Dict, Optional, Tuple

from swh.model.hashutil import MultiHash, HASH_BLOCK_SIZE
from swh.loader.package import DEFAULT_PARAMS


logger = logging.getLogger(__name__)


def api_info(url: str) -> Dict:
    """Basic api client to retrieve information on project. This deals with
       fetching json metadata about pypi projects.

    Args:
        url (str): The api url (e.g PyPI, npm, etc...)

    Raises:
        ValueError in case of query failures (for some reasons: 404, ...)

    Returns:
        The associated response's information dict

    """
    response = requests.get(url, **DEFAULT_PARAMS)
    if response.status_code != 200:
        raise ValueError("Fail to query '%s'. Reason: %s" % (
            url, response.status_code))
    return response.json()


def download(url: str, dest: str, hashes: Dict = {},
             filename: Optional[str] = None,
             auth: Optional[Tuple[str, str]] = None) -> Tuple[str, Dict]:
    """Download a remote tarball from url, uncompresses and computes swh hashes
       on it.

    Args:
        url: Artifact uri to fetch, uncompress and hash
        dest: Directory to write the archive to
        hashes: Dict of expected hashes (key is the hash algo) for the artifact
            to download (those hashes are expected to be hex string)
        auth: Optional tuple of login/password (for http authentication
            service, e.g. deposit)

    Raises:
        ValueError in case of any error when fetching/computing (length,
        checksums mismatched...)

    Returns:
        Tuple of local (filepath, hashes of filepath)

    """
    params = copy.deepcopy(DEFAULT_PARAMS)
    if auth is not None:
        params['auth'] = auth
    response = requests.get(url, **params, stream=True)
    logger.debug('headers: %s', response.headers)
    if response.status_code != 200:
        raise ValueError("Fail to query '%s'. Reason: %s" % (
            url, response.status_code))
    _length = response.headers.get('content-length')
    # some server do not provide the content-length header...
    length = int(_length) if _length is not None else len(response.content)

    filename = filename if filename else os.path.basename(url)
    logger.debug('filename: %s', filename)
    filepath = os.path.join(dest, filename)
    logger.debug('filepath: %s', filepath)

    h = MultiHash(length=length)
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=HASH_BLOCK_SIZE):
            h.update(chunk)
            f.write(chunk)

    actual_length = os.path.getsize(filepath)
    if length != actual_length:
        raise ValueError('Error when checking size: %s != %s' % (
            length, actual_length))

    # Also check the expected hashes if provided
    if hashes:
        actual_hashes = h.hexdigest()
        for algo_hash in hashes.keys():
            actual_digest = actual_hashes[algo_hash]
            expected_digest = hashes[algo_hash]
            if actual_digest != expected_digest:
                raise ValueError(
                    'Failure when fetching %s. '
                    'Checksum mismatched: %s != %s' % (
                        url, expected_digest, actual_digest))

    extrinsic_metadata = {
        'length': length,
        'filename': filename,
        'checksums': {
            **h.hexdigest()
        },
    }

    logger.debug('extrinsic_metadata', extrinsic_metadata)

    return filepath, extrinsic_metadata


def release_name(version: str, filename: Optional[str] = None) -> str:
    if filename:
        return 'releases/%s/%s' % (version, filename)
    return 'releases/%s' % version
