# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.model.hashutil import hash_to_bytes
from swh.model.model import Snapshot, SnapshotBranch, TargetType
from swh.loader.package.tests.common import (
    decode_target,
    check_snapshot,
    check_metadata,
    check_metadata_paths,
)
from swh.storage import get_storage

hash_hex = "43e45d56f88993aae6a0198013efa80716fd8920"


storage_config = {"cls": "pipeline", "steps": [{"cls": "memory",}]}


def test_decode_target_edge():
    assert not decode_target(None)


def test_decode_target():
    actual_alias_decode_target = decode_target(
        {"target_type": "alias", "target": b"something",}
    )

    assert actual_alias_decode_target == {
        "target_type": "alias",
        "target": "something",
    }

    actual_decode_target = decode_target(
        {"target_type": "revision", "target": hash_to_bytes(hash_hex),}
    )

    assert actual_decode_target == {
        "target_type": "revision",
        "target": hash_hex,
    }


def test_check_snapshot():
    storage = get_storage(**storage_config)

    snap_id = "2498dbf535f882bc7f9a18fb16c9ad27fda7bab7"
    snapshot = Snapshot(
        id=hash_to_bytes(snap_id),
        branches={
            b"master": SnapshotBranch(
                target=hash_to_bytes(hash_hex), target_type=TargetType.REVISION,
            ),
        },
    )

    s = storage.snapshot_add([snapshot])
    assert s == {
        "snapshot:add": 1,
    }

    expected_snapshot = {
        "id": snap_id,
        "branches": {"master": {"target": hash_hex, "target_type": "revision",}},
    }
    check_snapshot(expected_snapshot, storage)


def test_check_snapshot_failure():
    storage = get_storage(**storage_config)

    snapshot = Snapshot(
        id=hash_to_bytes("2498dbf535f882bc7f9a18fb16c9ad27fda7bab7"),
        branches={
            b"master": SnapshotBranch(
                target=hash_to_bytes(hash_hex), target_type=TargetType.REVISION,
            ),
        },
    )

    s = storage.snapshot_add([snapshot])
    assert s == {
        "snapshot:add": 1,
    }

    unexpected_snapshot = {
        "id": "2498dbf535f882bc7f9a18fb16c9ad27fda7bab7",
        "branches": {
            "master": {"target": hash_hex, "target_type": "release",}  # wrong value
        },
    }

    with pytest.raises(AssertionError):
        check_snapshot(unexpected_snapshot, storage)


def test_check_metadata():
    metadata = {
        "a": {"raw": {"time": "something",},},
        "b": [],
        "c": 1,
    }

    for raw_path, raw_type in [
        ("a.raw", dict),
        ("a.raw.time", str),
        ("b", list),
        ("c", int),
    ]:
        check_metadata(metadata, raw_path, raw_type)


def test_check_metadata_ko():
    metadata = {
        "a": {"raw": "hello",},
        "b": [],
        "c": 1,
    }

    for raw_path, raw_type in [
        ("a.b", dict),
        ("a.raw.time", str),
    ]:
        with pytest.raises(AssertionError):
            check_metadata(metadata, raw_path, raw_type)


def test_check_metadata_paths():
    metadata = {
        "a": {"raw": {"time": "something",},},
        "b": [],
        "c": 1,
    }

    check_metadata_paths(
        metadata, [("a.raw", dict), ("a.raw.time", str), ("b", list), ("c", int),]
    )


def test_check_metadata_paths_ko():
    metadata = {
        "a": {"raw": "hello",},
        "b": [],
        "c": 1,
    }

    with pytest.raises(AssertionError):
        check_metadata_paths(metadata, [("a.b", dict), ("a.raw.time", str),])
