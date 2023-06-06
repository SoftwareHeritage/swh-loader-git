# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dulwich.client import HTTPUnauthorized
from dulwich.errors import GitProtocolError, NotGitRepository
import pytest

from swh.loader.exception import NotFound
from swh.loader.git import utils


class TestUtils:
    def test_check_date_time(self):
        """A long as datetime is fine, date time check does not raise"""
        for e in range(32, 37):
            ts = 2**e
            utils.check_date_time(ts)

    def test_check_date_time_empty_value(self):
        assert utils.check_date_time(None) is None

    def test_check_date_time_raises(self):
        """From a give threshold, check will no longer works."""
        exp = 38
        timestamp = 2**exp
        with pytest.raises(ValueError, match=".*is out of range.*"):
            utils.check_date_time(timestamp)


def test_ignore_branch_name():
    branches = {
        b"HEAD",
        b"refs/heads/master",
        b"refs/{}",
        b"refs/pull/10/head",
        b"refs/pull/100/head",
        b"refs/pull/xyz/merge",  # auto-merged GitHub pull requests filtered out
        b"refs/^{}",  # Peeled refs filtered out
    }

    actual_branches = {b for b in branches if not utils.ignore_branch_name(b)}

    assert actual_branches == set(
        [
            b"HEAD",
            b"refs/heads/master",
            b"refs/{}",
            b"refs/pull/10/head",
            b"refs/pull/100/head",
        ]
    )


@pytest.mark.parametrize(
    "exception",
    [
        HTTPUnauthorized("url", "not authorized"),
        NotGitRepository("not a git repo"),
        GitProtocolError(" unavailable"),
        GitProtocolError(" not found"),
        GitProtocolError(" unexpected http resp 401"),
        GitProtocolError(" unexpected http resp 403"),
        GitProtocolError(" unexpected http resp 410"),
    ],
)
def test_raise_not_found_repository(exception, mocker):
    """Sensible not found exceptions are filtered as not found exception"""
    msg = exception.args[0]
    # All those exceptions are caught as not found (with the original error message
    # propagated)
    with pytest.raises(NotFound, match=msg):
        with utils.raise_not_found_repository():
            raise exception


@pytest.mark.parametrize("exception", [GitProtocolError, ValueError, Exception])
def test_raise_not_found_repository_raised_back(exception):
    """Any other kind of exceptions are just raised back."""
    exc = exception("this is raised back")
    with pytest.raises(exception, match="raised back"):
        with utils.raise_not_found_repository():
            raise exc
