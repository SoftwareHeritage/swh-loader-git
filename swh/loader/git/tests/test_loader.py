# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

import pytest
import dulwich.repo

from unittest import TestCase

from swh.loader.git.loader import GitLoader
from swh.loader.git.tests.test_from_disk import FullGitLoaderTests

from swh.loader.tests import prepare_repository_from_archive


class GitLoaderTest(TestCase, FullGitLoaderTests):
    """Prepare a git directory repository to be loaded through a GitLoader.
    This tests all git loader scenario.

    """

    @pytest.fixture(autouse=True)
    def init(self, swh_config, datadir, tmp_path):
        super().setUp()
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        self.repo_url = prepare_repository_from_archive(
            archive_path, archive_name, tmp_path=tmp_path
        )
        self.destination_path = os.path.join(tmp_path, archive_name)
        self.loader = GitLoader(self.repo_url)
        self.repo = dulwich.repo.Repo(self.destination_path)
