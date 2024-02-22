# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Utilities helper functions"""

from contextlib import contextmanager
import datetime
import logging
import os
import shutil
import tempfile
import time
from typing import Dict, Mapping, NewType, Optional, Union

from dulwich.client import HTTPUnauthorized
from dulwich.errors import GitProtocolError, NotGitRepository

from swh.core import tarball
from swh.loader.exception import NotFound
from swh.model.model import SnapshotBranch

# The hexadecimal representation of the hash in bytes
HexBytes = NewType("HexBytes", bytes)


def init_git_repo_from_archive(project_name, archive_path, root_temp_dir="/tmp"):
    """Given a path to an archive containing a git repository.

    Uncompress that archive to a temporary location and returns the path.

    If any problem whatsoever is raised, clean up the temporary location.

    Args:
        project_name (str): Project's name
        archive_path (str): Full path to the archive
        root_temp_dir (str): Optional temporary directory mount point
                             (default to /tmp)

    Returns
        A tuple:
        - temporary folder: containing the mounted repository
        - repo_path, path to the mounted repository inside the temporary folder

    Raises
        ValueError in case of failure to run the command to uncompress

    """
    temp_dir = tempfile.mkdtemp(
        suffix=".swh.loader.git", prefix="tmp.", dir=root_temp_dir
    )

    try:
        # create the repository that will be loaded with the dump
        tarball.uncompress(archive_path, temp_dir)
        repo_path = os.path.join(temp_dir, project_name)
        # tarball content may not be as expected (e.g. no top level directory
        # or a top level directory with a name different from project_name),
        # so try to make it loadable anyway
        if not os.path.exists(repo_path):
            os.mkdir(repo_path)
            for root, dirs, files in os.walk(temp_dir):
                if ".git" in dirs:
                    shutil.copytree(
                        os.path.join(root, ".git"), os.path.join(repo_path, ".git")
                    )
                    break
        return temp_dir, repo_path
    except Exception as e:
        shutil.rmtree(temp_dir)
        raise e


def check_date_time(timestamp):
    """Check date time for overflow errors.

    Args:
        timestamp (timestamp): Timestamp in seconds

    Raise:
        Any error raised by datetime fromtimestamp conversion error.

    """
    if not timestamp:
        return None
    datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


def ignore_branch_name(branch_name: bytes) -> bool:
    """Should the git loader ignore the branch named `branch_name`?"""
    if branch_name.endswith(b"^{}"):
        # Peeled refs make the git protocol explode
        return True
    elif branch_name.startswith(b"refs/pull/") and branch_name.endswith(b"/merge"):
        # We filter-out auto-merged GitHub pull requests
        return True

    return False


def filter_refs(refs: Mapping[bytes, Union[bytes, HexBytes]]) -> Dict[bytes, HexBytes]:
    """Filter the refs dictionary using the policy set in `ignore_branch_name`"""
    return {
        name: HexBytes(target)
        for name, target in refs.items()
        if not ignore_branch_name(name)
    }


def warn_dangling_branches(
    branches: Dict[bytes, Optional[SnapshotBranch]],
    dangling_branches: Dict[HexBytes, bytes],
    logger: logging.Logger,
    origin_url: str,
) -> None:
    dangling_branches = {
        target: ref for target, ref in dangling_branches.items() if not branches[target]
    }

    if dangling_branches:
        descr = [f"{ref!r}->{target!r}" for target, ref in dangling_branches.items()]

        logger.warning(
            "Dangling symbolic references: %s",
            ", ".join(descr),
            extra={
                "swh_type": "swh_loader_git_dangling_symrefs",
                "swh_refs": descr,
                "origin_url": origin_url,
            },
        )


@contextmanager
def raise_not_found_repository():
    """Catches all kinds of exceptions which translate to an inexistent repository and
    reraise as a NotFound exception. Any other exceptions are propagated to the caller.

    Raises:
        NotFound: instead of HTTPUnauthorized, NotGitRepository and any GitProtocol with
            specific error message relative to an inexistent repository.
        *: Any other exceptions raised within the try block

    """
    try:
        yield
    except (HTTPUnauthorized, NotGitRepository) as e:
        raise NotFound(e)
    except GitProtocolError as e:
        # that kind of error is unfortunately not specific to a not found scenario... It
        # depends on the value of message within the exception. So parse the exception
        # message to detect if it's a not found or not.
        for msg in [
            " unavailable",  # e.g DMCA takedown
            " not found",
            "unexpected http resp 401",
            "unexpected http resp 403",
            "unexpected http resp 410",
        ]:
            if msg in str(e.args[0]):
                raise NotFound(e)
        # otherwise transmit the error
        raise


# How often to log messages for long-running operations, in seconds
LOGGING_INTERVAL = 30


class PackWriter:
    """Helper class to abort git loading if pack file currently downloaded
    has a size in bytes that exceeds a given threshold."""

    def __init__(
        self,
        pack_buffer: tempfile.SpooledTemporaryFile,
        size_limit: int,
        origin_url: str,
        fetch_pack_logger: logging.Logger,
    ):
        self.pack_buffer = pack_buffer
        self.size_limit = size_limit
        self.origin_url = origin_url
        self.fetch_pack_logger = fetch_pack_logger
        self.last_time_logged = time.monotonic()

    def write(self, data: bytes):
        cur_size = self.pack_buffer.tell()
        would_write = len(data)
        fetched = cur_size + would_write
        if fetched > self.size_limit:
            raise IOError(
                f"Pack file too big for repository {self.origin_url}, "
                f"limit is {self.size_limit} bytes, current size is {cur_size}, "
                f"would write {would_write}"
            )

        if time.monotonic() > self.last_time_logged + LOGGING_INTERVAL:
            self.fetch_pack_logger.info(
                "Fetched %s packfile bytes so far (%.2f%% of configured limit)",
                fetched,
                100 * fetched / self.size_limit,
            )
            self.last_time_logged = time.monotonic()

        self.pack_buffer.write(data)
