# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Optional

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
    if snapshot:
        assert visit_status.snapshot == snapshot, (
            "Visit_status points to snapshot {visit_status.snapshot!r} "
            f"instead of {snapshot!r}"
        )
    return visit_status
