# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import path

import logging

from typing import Dict, List, Tuple

from swh.model.hashutil import hash_to_bytes, hash_to_hex

logger = logging.getLogger(__file__)


DATADIR = path.join(path.abspath(path.dirname(__file__)), "resources")


def decode_target(target):
    """Test helper to ease readability in test

    """
    if not target:
        return target
    target_type = target["target_type"]

    if target_type == "alias":
        decoded_target = target["target"].decode("utf-8")
    else:
        decoded_target = hash_to_hex(target["target"])

    return {"target": decoded_target, "target_type": target_type}


def check_snapshot(expected_snapshot, storage):
    """Check for snapshot match.

    Provide the hashes as hexadecimal, the conversion is done
    within the method.

    Args:
        expected_snapshot (dict): full snapshot with hex ids
        storage (Storage): expected storage

    Returns:
        the snapshot stored in the storage for further test assertion if any is
        needed.

    """
    expected_snapshot_id = expected_snapshot["id"]
    expected_branches = expected_snapshot["branches"]
    snap = storage.snapshot_get(hash_to_bytes(expected_snapshot_id))
    if snap is None:
        # display known snapshots instead if possible
        if hasattr(storage, "_snapshots"):  # in-mem storage
            from pprint import pprint

            for snap_id, (_snap, _) in storage._snapshots.items():
                snapd = _snap.to_dict()
                snapd["id"] = hash_to_hex(snapd["id"])
                branches = {
                    branch.decode("utf-8"): decode_target(target)
                    for branch, target in snapd["branches"].items()
                }
                snapd["branches"] = branches
                pprint(snapd)
        raise AssertionError("Snapshot is not found")

    branches = {
        branch.decode("utf-8"): decode_target(target)
        for branch, target in snap["branches"].items()
    }
    assert expected_branches == branches
    return snap


def check_metadata(metadata: Dict, key_path: str, raw_type: str):
    """Given a metadata dict, ensure the associated key_path value is of type
       raw_type.

    Args:
        metadata: Dict to check
        key_path: Path to check
        raw_type: Type to check the path with

    Raises:
        Assertion error in case of mismatch

    """
    data = metadata
    keys = key_path.split(".")
    for k in keys:
        try:
            data = data[k]
        except (TypeError, KeyError) as e:
            # KeyError: because path too long
            # TypeError: data is not a dict
            raise AssertionError(e)
    assert isinstance(data, raw_type)  # type: ignore


def check_metadata_paths(metadata: Dict, paths: List[Tuple[str, str]]):
    """Given a metadata dict, ensure the keys are of expected types

    Args:
        metadata: Dict to check
        key_path: Path to check
        raw_type: Type to check the path with

    Raises:
        Assertion error in case of mismatch

    """
    for key_path, raw_type in paths:
        check_metadata(metadata, key_path, raw_type)


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
