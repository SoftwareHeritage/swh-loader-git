# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from dulwich.errors import GitProtocolError, NotGitRepository, ObjectFormatException
import dulwich.repo
import pytest

from swh.loader.git.loader import GitLoader
from swh.loader.git.tests.test_from_disk import FullGitLoaderTests
from swh.loader.tests import assert_last_visit_matches, prepare_repository_from_archive


class CommonGitLoaderNotFound:
    @pytest.fixture(autouse=True)
    def __inject_fixtures(self, mocker):
        """Inject required fixtures in unittest.TestCase class

        """
        self.mocker = mocker

    @pytest.mark.parametrize(
        "failure_exception",
        [
            GitProtocolError("Repository unavailable"),  # e.g DMCA takedown
            GitProtocolError("Repository not found"),
            GitProtocolError("unexpected http resp 401"),
            NotGitRepository("not a git repo"),
        ],
    )
    def test_load_visit_not_found(self, failure_exception):
        """Ingesting an unknown url result in a visit with not_found status

        """
        # simulate an initial communication error (e.g no repository found, ...)
        mock = self.mocker.patch(
            "swh.loader.git.loader.GitLoader.fetch_pack_from_origin"
        )
        mock.side_effect = failure_exception

        res = self.loader.load()
        assert res == {"status": "uneventful"}

        assert_last_visit_matches(
            self.loader.storage,
            self.repo_url,
            status="not_found",
            type="git",
            snapshot=None,
        )

    @pytest.mark.parametrize(
        "failure_exception",
        [IOError, ObjectFormatException, OSError, ValueError, GitProtocolError,],
    )
    def test_load_visit_failure(self, failure_exception):
        """Failing during the fetch pack step result in failing visit

        """
        # simulate a fetch communication error after the initial connection
        # server error (e.g IOError, ObjectFormatException, ...)
        mock = self.mocker.patch(
            "swh.loader.git.loader.GitLoader.fetch_pack_from_origin"
        )

        mock.side_effect = failure_exception("failure")

        res = self.loader.load()
        assert res == {"status": "failed"}

        assert_last_visit_matches(
            self.loader.storage,
            self.repo_url,
            status="failed",
            type="git",
            snapshot=None,
        )


class TestGitLoader(FullGitLoaderTests, CommonGitLoaderNotFound):
    """Prepare a git directory repository to be loaded through a GitLoader.
    This tests all git loader scenario.

    """

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path):
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        self.repo_url = prepare_repository_from_archive(
            archive_path, archive_name, tmp_path=tmp_path
        )
        self.destination_path = os.path.join(tmp_path, archive_name)
        self.loader = GitLoader(swh_storage, self.repo_url)
        self.repo = dulwich.repo.Repo(self.destination_path)


class TestGitLoader2(FullGitLoaderTests, CommonGitLoaderNotFound):
    """Mostly the same loading scenario but with a base-url different than the repo-url.
    To walk slightly different paths, the end result should stay the same.

    """

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path):
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        self.repo_url = prepare_repository_from_archive(
            archive_path, archive_name, tmp_path=tmp_path
        )
        self.destination_path = os.path.join(tmp_path, archive_name)
        base_url = f"base://{self.repo_url}"
        self.loader = GitLoader(swh_storage, self.repo_url, base_url=base_url)
        self.repo = dulwich.repo.Repo(self.destination_path)
