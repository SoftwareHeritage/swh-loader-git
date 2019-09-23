# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import requests

from typing import Dict, Tuple

from swh.model.hashutil import MultiHash, HASH_BLOCK_SIZE
from swh.loader.package import DEFAULT_PARAMS


def download(url: str, dest: str) -> Tuple[str, Dict]:
    """Download a remote tarball from url, uncompresses and computes swh hashes
       on it.

    Args:
        url: Artifact uri to fetch, uncompress and hash
        dest: Directory to write the archive to

    Raises:
        ValueError in case of any error when fetching/computing

    Returns:
        Tuple of local (filepath, hashes of filepath)

    """
    response = requests.get(url, **DEFAULT_PARAMS, stream=True)
    if response.status_code != 200:
        raise ValueError("Fail to query '%s'. Reason: %s" % (
            url, response.status_code))
    length = int(response.headers['content-length'])

    filepath = os.path.join(dest, os.path.basename(url))

    h = MultiHash(length=length)
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=HASH_BLOCK_SIZE):
            h.update(chunk)
            f.write(chunk)

    actual_length = os.path.getsize(filepath)
    if length != actual_length:
        raise ValueError('Error when checking size: %s != %s' % (
            length, actual_length))

    # hashes = h.hexdigest()
    # actual_digest = hashes['sha256']
    # if actual_digest != artifact['sha256']:
    #     raise ValueError(
    #         '%s %s: Checksum mismatched: %s != %s' % (
    #             project, version, artifact['sha256'], actual_digest))

    return filepath, {
        'length': length,
        **h.hexdigest()
    }
