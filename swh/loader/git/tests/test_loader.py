# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.git.loader import GitLoader
from swh.loader.git.tests.test_from_disk import DirGitLoaderTest

from . import TEST_LOADER_CONFIG


class GitLoaderTest(GitLoader):
    def parse_config_file(self, *args, **kwargs):
        return {
            **super().parse_config_file(*args, **kwargs),
            **TEST_LOADER_CONFIG
        }


class TestGitLoader(DirGitLoaderTest):
    """Same tests as for the GitLoaderFromDisk, but running on GitLoader."""
    def setUp(self):
        super().setUp()
        self.loader = GitLoaderTest(self.repo_url)
        self.storage = self.loader.storage

    def load(self):
        return self.loader.load()
