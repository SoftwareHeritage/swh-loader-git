# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model.model import Snapshot, SnapshotBranch, TargetType

from swh.loader.git.loader import GitLoader
from swh.loader.git.tests.test_from_disk import SNAPSHOT1, DirGitLoaderTest

from . import TEST_LOADER_CONFIG


class GitLoaderTest(GitLoader):
    def parse_config_file(self, *args, **kwargs):
        return {**super().parse_config_file(*args, **kwargs), **TEST_LOADER_CONFIG}


class TestGitLoader(DirGitLoaderTest):
    """Same tests as for the GitLoaderFromDisk, but running on GitLoader."""

    def setUp(self):
        super().setUp()
        self.loader = GitLoaderTest(self.repo_url)
        self.storage = self.loader.storage

    def load(self):
        return self.loader.load()

    def test_load_filter_branches(self):
        filtered_branches = {b"refs/pull/42/merge"}
        unfiltered_branches = {b"refs/pull/42/head"}

        # Add branches to the repository on disk; some should be filtered by
        # the loader, some should not.
        for branch_name in filtered_branches | unfiltered_branches:
            self.repo[branch_name] = self.repo[b"refs/heads/master"]

        # Generate the expected snapshot from SNAPSHOT1 (which is the original
        # state of the git repo)...
        branches = {}

        for branch_name, branch_dict in SNAPSHOT1["branches"].items():
            target_type_name = branch_dict["target_type"]
            target_obj = branch_dict["target"]

            if target_type_name != "alias":
                target = bytes.fromhex(target_obj)
            else:
                target = target_obj.encode()

            branch = SnapshotBranch(
                target=target, target_type=TargetType(target_type_name)
            )
            branches[branch_name.encode()] = branch

        # ... and the unfiltered_branches, which are all pointing to the same
        # commit as "refs/heads/master".
        for branch_name in unfiltered_branches:
            branches[branch_name] = branches[b"refs/heads/master"]

        expected_snapshot = Snapshot(branches=branches)

        # Load the modified repository
        res = self.load()
        assert res["status"] == "eventful"

        assert self.loader.load_status() == {"status": "eventful"}
        assert self.loader.visit_status() == "full"

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        assert visit["snapshot"] == expected_snapshot.id
        assert visit["status"] == "full"
