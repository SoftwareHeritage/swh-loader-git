# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import pytest

from swh.loader.tests.common import assert_last_visit_matches
from swh.model.model import OriginVisit, OriginVisitStatus
from swh.model.hashutil import hash_to_bytes


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
    mock_storage = mocker.patch(
        "swh.loader.tests.common.origin_get_latest_visit_status"
    )
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
