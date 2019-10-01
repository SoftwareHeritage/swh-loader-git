# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import requests

from typing import Dict, Tuple

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


def download(url: str, dest: str, hashes: Dict = {}) -> Tuple[str, Dict]:
    """Download a remote tarball from url, uncompresses and computes swh hashes
       on it.

    Args:
        url: Artifact uri to fetch, uncompress and hash
        dest: Directory to write the archive to

        hashes: Dict of expected hashes (key is the hash algo) for the artifact
            to download (those hashes are expected to be hex string)

    Raises:
        ValueError in case of any error when fetching/computing (length,
        checksums mismatched...)

    Returns:
        Tuple of local (filepath, hashes of filepath)

    """
    response = requests.get(url, **DEFAULT_PARAMS, stream=True)
    if response.status_code != 200:
        raise ValueError("Fail to query '%s'. Reason: %s" % (
            url, response.status_code))
    length = int(response.headers['content-length'])

    filename = os.path.basename(url)
    filepath = os.path.join(dest, filename)

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
