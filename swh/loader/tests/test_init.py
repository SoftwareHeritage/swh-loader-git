# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import pytest

import os
import subprocess

from swh.loader.tests import prepare_repository_from_archive, assert_last_visit_matches
from swh.model.model import (
    OriginVisit,
    OriginVisitStatus,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.model.hashutil import hash_to_bytes

from swh.loader.tests import (
    decode_target,
    check_snapshot,
)


hash_hex = "43e45d56f88993aae6a0198013efa80716fd8920"


ORIGIN_VISIT = OriginVisit(
    origin="some-url",
    visit=1,
    date=datetime.datetime.now(tz=datetime.timezone.utc),
    type="archive",
)


ORIGIN_VISIT_STATUS = OriginVisitStatus(
    origin="some-url",
    visit=1,
    date=datetime.datetime.now(tz=datetime.timezone.utc),
    status="full",
    snapshot=hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
    metadata=None,
)


@pytest.fixture
def mock_storage(mocker):
    mock_storage = mocker.patch("swh.loader.tests.origin_get_latest_visit_status")
    mock_storage.return_value = ORIGIN_VISIT, ORIGIN_VISIT_STATUS
    return mock_storage


def test_assert_last_visit_matches_raise(mock_storage, mocker):
    """Not finding origin visit_and_statu should raise

    """
    # overwrite so we raise because we do not find the right visit
    mock_storage.return_value = None

    with pytest.raises(AssertionError, match="Origin url has no visits"):
        assert_last_visit_matches(mock_storage, "url", status="full")

    assert mock_storage.called is True


def test_assert_last_visit_matches_wrong_status(mock_storage, mocker):
    """Wrong visit detected should raise AssertionError

    """
    expected_status = "partial"
    assert ORIGIN_VISIT_STATUS.status != expected_status
    with pytest.raises(AssertionError, match="Visit_status has status"):
        assert_last_visit_matches(mock_storage, "url", status=expected_status)

    assert mock_storage.called is True


def test_assert_last_visit_matches_wrong_type(mock_storage, mocker):
    """Wrong visit detected should raise AssertionError

    """
    expected_type = "git"
    assert ORIGIN_VISIT.type != expected_type
    with pytest.raises(AssertionError, match="Visit has type"):
        assert_last_visit_matches(
            mock_storage,
            "url",
            status=ORIGIN_VISIT_STATUS.status,
            type=expected_type,  # mismatched type will raise
        )

    assert mock_storage.called is True


def test_assert_last_visit_matches_wrong_snapshot(mock_storage, mocker):
    """Wrong visit detected should raise AssertionError

    """
    expected_snapshot_id = hash_to_bytes("e92cc0710eb6cf9efd5b920a8453e1e07157b6cd")
    assert ORIGIN_VISIT_STATUS.snapshot != expected_snapshot_id

    with pytest.raises(AssertionError, match="Visit_status points to snapshot"):
        assert_last_visit_matches(
            mock_storage,
            "url",
            status=ORIGIN_VISIT_STATUS.status,
            snapshot=expected_snapshot_id,  # mismatched snapshot will raise
        )

    assert mock_storage.called is True


def test_assert_last_visit_matches(mock_storage, mocker):
    """Correct visit detected should return the visit_status

    """
    visit_type = ORIGIN_VISIT.type
    visit_status = ORIGIN_VISIT_STATUS.status
    visit_snapshot = ORIGIN_VISIT_STATUS.snapshot

    actual_visit_status = assert_last_visit_matches(
        mock_storage,
        "url",
        type=visit_type,
        status=visit_status,
        snapshot=visit_snapshot,
    )

    assert actual_visit_status == ORIGIN_VISIT_STATUS
    assert mock_storage.called is True


def test_prepare_repository_from_archive_failure():
    # does not deal with inexistent archive so raise
    assert os.path.exists("unknown-archive") is False
    with pytest.raises(subprocess.CalledProcessError, match="exit status 2"):
        prepare_repository_from_archive("unknown-archive")


def test_prepare_repository_from_archive(datadir, tmp_path):
    archive_name = "0805nexter-1.1.0"
    archive_path = os.path.join(str(datadir), f"{archive_name}.tar.gz")
    assert os.path.exists(archive_path) is True

    tmp_path = str(tmp_path)  # deals with path string
    repo_url = prepare_repository_from_archive(
        archive_path, filename=archive_name, tmp_path=tmp_path
    )
    expected_uncompressed_archive_path = os.path.join(tmp_path, archive_name)
    assert repo_url == f"file://{expected_uncompressed_archive_path}"
    assert os.path.exists(expected_uncompressed_archive_path)


def test_prepare_repository_from_archive_no_filename(datadir, tmp_path):
    archive_name = "0805nexter-1.1.0"
    archive_path = os.path.join(str(datadir), f"{archive_name}.tar.gz")
    assert os.path.exists(archive_path) is True

    # deals with path as posix path (for tmp_path)
    repo_url = prepare_repository_from_archive(archive_path, tmp_path=tmp_path)

    tmp_path = str(tmp_path)
    expected_uncompressed_archive_path = os.path.join(tmp_path, archive_name)
    expected_repo_url = os.path.join(tmp_path, f"{archive_name}.tar.gz")
    assert repo_url == f"file://{expected_repo_url}"

    # passing along the filename does not influence the on-disk extraction
    # just the repo-url computation
    assert os.path.exists(expected_uncompressed_archive_path)


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


def test_check_snapshot(swh_storage):
    snap_id = "2498dbf535f882bc7f9a18fb16c9ad27fda7bab7"
    snapshot = Snapshot(
        id=hash_to_bytes(snap_id),
        branches={
            b"master": SnapshotBranch(
                target=hash_to_bytes(hash_hex), target_type=TargetType.REVISION,
            ),
        },
    )

    s = swh_storage.snapshot_add([snapshot])
    assert s == {
        "snapshot:add": 1,
    }

    expected_snapshot = {
        "id": snap_id,
        "branches": {"master": {"target": hash_hex, "target_type": "revision",}},
    }
    check_snapshot(expected_snapshot, swh_storage)


def test_check_snapshot_failure(swh_storage):
    snapshot = Snapshot(
        id=hash_to_bytes("2498dbf535f882bc7f9a18fb16c9ad27fda7bab7"),
        branches={
            b"master": SnapshotBranch(
                target=hash_to_bytes(hash_hex), target_type=TargetType.REVISION,
            ),
        },
    )

    s = swh_storage.snapshot_add([snapshot])
    assert s == {
        "snapshot:add": 1,
    }

    unexpected_snapshot = {
        "id": "2498dbf535f882bc7f9a18fb16c9ad27fda7bab7",  # id is correct
        "branches": {
            "master": {"target": hash_hex, "target_type": "release",}  # wrong branch
        },
    }

    with pytest.raises(AssertionError, match="Differing items"):
        check_snapshot(unexpected_snapshot, swh_storage)

    # snapshot id which does not exist
    unexpected_snapshot["id"] = "999666f535f882bc7f9a18fb16c9ad27fda7bab7"
    with pytest.raises(AssertionError, match="is not found"):
        check_snapshot(unexpected_snapshot, swh_storage)
