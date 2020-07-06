# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import subprocess

from pathlib import PosixPath
from typing import Optional, Union
from swh.model.model import OriginVisitStatus

from swh.storage.algos.origin import origin_get_latest_visit_status


def assert_last_visit_matches(
    storage,
    url: str,
    status: str,
    type: Optional[str] = None,
    snapshot: Optional[bytes] = None,
) -> OriginVisitStatus:
    """This retrieves the last visit and visit_status which are expected to exist.

    This also checks that the {visit|visit_status} have their respective properties
    correctly set.

    This returns the last visit_status for that given origin.

    Args:
        url: Origin url
        status: Check that the visit status has the given status
        type: Check that the returned visit has the given type
        snapshot: Check that the visit status points to the given snapshot

    Raises:
        AssertionError in case visit or visit status is not found, or any of the type,
        status and snapshot mismatch

    Returns:
        the visit status for further check during the remaining part of the test.

    """
    visit_and_status = origin_get_latest_visit_status(storage, url)
    assert visit_and_status is not None, f"Origin {url} has no visits"
    visit, visit_status = visit_and_status
    if type:
        assert visit.type == type, f"Visit has type {visit.type} instead of {type}"
    assert (
        visit_status.status == status
    ), f"Visit_status has status {visit_status.status} instead of {status}"
    if snapshot is not None:
        assert visit_status.snapshot is not None
        assert visit_status.snapshot == snapshot, (
            f"Visit_status points to snapshot {visit_status.snapshot.hex()} "
            f"instead of {snapshot.hex()}"
        )
    return visit_status


def prepare_repository_from_archive(
    archive_path: str,
    filename: Optional[str] = None,
    tmp_path: Union[PosixPath, str] = "/tmp",
) -> str:
    """Given an existing archive_path, uncompress it.
    Returns a file repo url which can be used as origin url.

    This does not deal with the case where the archive passed along does not exist.

    """
    if not isinstance(tmp_path, str):
        tmp_path = str(tmp_path)
    # uncompress folder/repositories/dump for the loader to ingest
    subprocess.check_output(["tar", "xf", archive_path, "-C", tmp_path])
    # build the origin url (or some derivative form)
    _fname = filename if filename else os.path.basename(archive_path)
    repo_url = f"file://{tmp_path}/{_fname}"
    return repo_url
