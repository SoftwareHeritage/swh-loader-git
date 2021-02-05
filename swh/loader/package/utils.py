# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import functools
import logging
import os
from typing import Callable, Dict, Optional, Tuple, TypeVar

import requests

from swh.loader.exception import NotFound
from swh.loader.package import DEFAULT_PARAMS
from swh.model.hashutil import HASH_BLOCK_SIZE, MultiHash
from swh.model.model import Person

logger = logging.getLogger(__name__)


DOWNLOAD_HASHES = set(["sha1", "sha256", "length"])


EMPTY_AUTHOR = Person(fullname=b"", name=None, email=None,)


def api_info(url: str, **extra_params) -> bytes:
    """Basic api client to retrieve information on project. This deals with
       fetching json metadata about pypi projects.

    Args:
        url (str): The api url (e.g PyPI, npm, etc...)

    Raises:
        NotFound in case of query failures (for some reasons: 404, ...)

    Returns:
        The associated response's information

    """
    response = requests.get(url, **{**DEFAULT_PARAMS, **extra_params})
    if response.status_code != 200:
        raise NotFound(f"Fail to query '{url}'. Reason: {response.status_code}")
    return response.content


def download(
    url: str,
    dest: str,
    hashes: Dict = {},
    filename: Optional[str] = None,
    auth: Optional[Tuple[str, str]] = None,
) -> Tuple[str, Dict]:
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
        params["auth"] = auth
    # so the connection does not hang indefinitely (read/connection timeout)
    timeout = params.get("timeout", 60)
    response = requests.get(url, **params, timeout=timeout, stream=True)
    if response.status_code != 200:
        raise ValueError("Fail to query '%s'. Reason: %s" % (url, response.status_code))

    filename = filename if filename else os.path.basename(url)
    logger.debug("filename: %s", filename)
    filepath = os.path.join(dest, filename)
    logger.debug("filepath: %s", filepath)

    h = MultiHash(hash_names=DOWNLOAD_HASHES)
    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=HASH_BLOCK_SIZE):
            h.update(chunk)
            f.write(chunk)

    # Also check the expected hashes if provided
    if hashes:
        actual_hashes = h.hexdigest()
        for algo_hash in hashes.keys():
            actual_digest = actual_hashes[algo_hash]
            expected_digest = hashes[algo_hash]
            if actual_digest != expected_digest:
                raise ValueError(
                    "Failure when fetching %s. "
                    "Checksum mismatched: %s != %s"
                    % (url, expected_digest, actual_digest)
                )

    computed_hashes = h.hexdigest()
    length = computed_hashes.pop("length")
    extrinsic_metadata = {
        "length": length,
        "filename": filename,
        "checksums": computed_hashes,
        "url": url,
    }

    logger.debug("extrinsic_metadata", extrinsic_metadata)

    return filepath, extrinsic_metadata


def release_name(version: str, filename: Optional[str] = None) -> str:
    if filename:
        return "releases/%s/%s" % (version, filename)
    return "releases/%s" % version


TReturn = TypeVar("TReturn")
TSelf = TypeVar("TSelf")

_UNDEFINED = object()


def cached_method(f: Callable[[TSelf], TReturn]) -> Callable[[TSelf], TReturn]:
    cache_name = f"_cached_{f.__name__}"

    @functools.wraps(f)
    def newf(self):
        value = getattr(self, cache_name, _UNDEFINED)
        if value is _UNDEFINED:
            value = f(self)
            setattr(self, cache_name, value)
        return value

    return newf
