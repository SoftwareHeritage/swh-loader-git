# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import subprocess

from pathlib import PosixPath
from typing import Any, Dict, Optional, Union

from swh.model.model import OriginVisitStatus, Snapshot
from swh.model.hashutil import hash_to_bytes

from swh.storage.interface import StorageInterface
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


def encode_target(target: Dict) -> Dict:
    """Test helper to ease readability in test

    """
    if not target:
        return target
    target_type = target["target_type"]
    target_data = target["target"]
    if target_type == "alias" and isinstance(target_data, str):
        encoded_target = target_data.encode("utf-8")
    elif isinstance(target_data, str):
        encoded_target = hash_to_bytes(target_data)
    else:
        encoded_target = target_data

    return {"target": encoded_target, "target_type": target_type}


def check_snapshot(
    snapshot: Union[Dict[str, Any], Snapshot], storage: StorageInterface
):
    """Check for snapshot match.

    The hashes can be both in hex or bytes, the necessary conversion will happen prior
    to check.

    Args:
        snapshot: full snapshot to check for existence and consistency
        storage: storage to lookup information into

    Returns:
        the snapshot stored in the storage for further test assertion if any is
        needed.

    """
    if isinstance(snapshot, Snapshot):
        expected_snapshot = snapshot
    elif isinstance(snapshot, dict):
        # dict must be snapshot compliant
        snapshot_dict = {"id": hash_to_bytes(snapshot["id"])}
        branches = {}
        for branch, target in snapshot["branches"].items():
            if isinstance(branch, str):
                branch = branch.encode("utf-8")
            branches[branch] = encode_target(target)
        snapshot_dict["branches"] = branches
        expected_snapshot = Snapshot.from_dict(snapshot_dict)
    else:
        raise AssertionError(f"variable 'snapshot' must be a snapshot: {snapshot!r}")

    snap = storage.snapshot_get(expected_snapshot.id)
    if snap is None:
        raise AssertionError(f"Snapshot {expected_snapshot.id.hex()} is not found")

    assert snap["next_branch"] is None  # we don't deal with large snapshot in tests
    snap.pop("next_branch")
    actual_snap = Snapshot.from_dict(snap)

    assert expected_snapshot == actual_snap

    return snap  # for retro compat, returned the dict, remove when clients are migrated


def get_stats(storage) -> Dict:
    """Adaptation utils to unify the stats counters across storage
       implementation.

    """
    storage.refresh_stat_counters()
    stats = storage.stat_counters()

    keys = [
        "content",
        "directory",
        "origin",
        "origin_visit",
        "person",
        "release",
        "revision",
        "skipped_content",
        "snapshot",
    ]
    return {k: stats.get(k) for k in keys}
