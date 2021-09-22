# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import os.path

import dulwich.objects
import dulwich.porcelain
import dulwich.repo
import pytest

from swh.loader.git.from_disk import GitLoaderFromArchive, GitLoaderFromDisk
from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
    prepare_repository_from_archive,
)
from swh.model.hashutil import bytehex_to_hash, hash_to_bytes
from swh.model.model import ObjectType, Release, Snapshot, SnapshotBranch, TargetType
from swh.storage.algos.snapshot import snapshot_get_all_branches

SNAPSHOT1 = Snapshot(
    id=hash_to_bytes("a23699280a82a043f8c0994cf1631b568f716f95"),
    branches={
        b"HEAD": SnapshotBranch(
            target=b"refs/heads/master", target_type=TargetType.ALIAS,
        ),
        b"refs/heads/master": SnapshotBranch(
            target=hash_to_bytes("2f01f5ca7e391a2f08905990277faf81e709a649"),
            target_type=TargetType.REVISION,
        ),
        b"refs/heads/branch1": SnapshotBranch(
            target=hash_to_bytes("b0a77609903f767a2fd3d769904ef9ef68468b87"),
            target_type=TargetType.REVISION,
        ),
        b"refs/heads/branch2": SnapshotBranch(
            target=hash_to_bytes("bd746cd1913721b269b395a56a97baf6755151c2"),
            target_type=TargetType.REVISION,
        ),
        b"refs/tags/branch2-after-delete": SnapshotBranch(
            target=hash_to_bytes("bd746cd1913721b269b395a56a97baf6755151c2"),
            target_type=TargetType.REVISION,
        ),
        b"refs/tags/branch2-before-delete": SnapshotBranch(
            target=hash_to_bytes("1135e94ccf73b5f9bd6ef07b3fa2c5cc60bba69b"),
            target_type=TargetType.REVISION,
        ),
    },
)

# directory hashes obtained with:
# gco b6f40292c4e94a8f7e7b4aff50e6c7429ab98e2a
# swh-hashtree --ignore '.git' --path .
# gco 2f01f5ca7e391a2f08905990277faf81e709a649
# swh-hashtree --ignore '.git' --path .
# gco bcdc5ebfde1a3cd6c96e0c2ea4eed19c13208777
# swh-hashtree --ignore '.git' --path .
# gco 1135e94ccf73b5f9bd6ef07b3fa2c5cc60bba69b
# swh-hashtree --ignore '.git' --path .
# gco 79f65ac75f79dda6ff03d66e1242702ab67fb51c
# swh-hashtree --ignore '.git' --path .
# gco b0a77609903f767a2fd3d769904ef9ef68468b87
# swh-hashtree --ignore '.git' --path .
# gco bd746cd1913721b269b395a56a97baf6755151c2
# swh-hashtree --ignore '.git' --path .
REVISIONS1 = {
    "b6f40292c4e94a8f7e7b4aff50e6c7429ab98e2a": (
        "40dbdf55dfd4065422462cc74a949254aefa972e"
    ),
    "2f01f5ca7e391a2f08905990277faf81e709a649": (
        "e1d0d894835f91a0f887a4bc8b16f81feefdfbd5"
    ),
    "bcdc5ebfde1a3cd6c96e0c2ea4eed19c13208777": (
        "b43724545b4759244bb54be053c690649161411c"
    ),
    "1135e94ccf73b5f9bd6ef07b3fa2c5cc60bba69b": (
        "fbf70528223d263661b5ad4b80f26caf3860eb8e"
    ),
    "79f65ac75f79dda6ff03d66e1242702ab67fb51c": (
        "5df34ec74d6f69072d9a0a6677d8efbed9b12e60"
    ),
    "b0a77609903f767a2fd3d769904ef9ef68468b87": (
        "9ca0c7d6ffa3f9f0de59fd7912e08f11308a1338"
    ),
    "bd746cd1913721b269b395a56a97baf6755151c2": (
        "e1d0d894835f91a0f887a4bc8b16f81feefdfbd5"
    ),
}


class CommonGitLoaderTests:
    """Common tests for all git loaders."""

    def test_load(self):
        """Loads a simple repository (made available by `setUp()`),
        and checks everything was added in the storage."""
        res = self.loader.load()

        assert res == {"status": "eventful"}

        assert_last_visit_matches(
            self.loader.storage,
            self.repo_url,
            status="full",
            type="git",
            snapshot=SNAPSHOT1.id,
        )

        stats = get_stats(self.loader.storage)
        assert stats == {
            "content": 4,
            "directory": 7,
            "origin": 1,
            "origin_visit": 1,
            "release": 0,
            "revision": 7,
            "skipped_content": 0,
            "snapshot": 1,
        }

        check_snapshot(SNAPSHOT1, self.loader.storage)

    def test_load_unchanged(self):
        """Checks loading a repository a second time does not add
        any extra data."""
        res = self.loader.load()
        assert res == {"status": "eventful"}

        assert_last_visit_matches(
            self.loader.storage,
            self.repo_url,
            status="full",
            type="git",
            snapshot=SNAPSHOT1.id,
        )

        stats0 = get_stats(self.loader.storage)
        assert stats0 == {
            "content": 4,
            "directory": 7,
            "origin": 1,
            "origin_visit": 1,
            "release": 0,
            "revision": 7,
            "skipped_content": 0,
            "snapshot": 1,
        }

        res = self.loader.load()
        assert res == {"status": "uneventful"}
        stats1 = get_stats(self.loader.storage)
        expected_stats = copy.deepcopy(stats0)
        expected_stats["origin_visit"] += 1
        assert stats1 == expected_stats

        check_snapshot(SNAPSHOT1, self.loader.storage)

        assert_last_visit_matches(
            self.loader.storage,
            self.repo_url,
            status="full",
            type="git",
            snapshot=SNAPSHOT1.id,
        )

    def test_load_visit_without_snapshot_so_status_failed(self):
        # unfortunately, monkey-patch the hard way, self.loader is already instantiated
        # (patching won't work self.loader is already instantiated)
        # Make get_contents fail for some reason
        self.loader.get_contents = None

        res = self.loader.load()
        assert res == {"status": "failed"}

        assert_last_visit_matches(
            self.loader.storage,
            self.repo_url,
            status="failed",
            type="git",
            snapshot=None,
        )

    def test_load_visit_with_snapshot_so_status_partial(self):
        # unfortunately, monkey-patch the hard way, self.loader is already instantiated
        # (patching won't work self.loader is already instantiated)
        # fake store_metadata raising for some reason, so we could have a snapshot id
        # at this point in time
        self.loader.store_metadata = None
        # fake having a snapshot so the visit status is partial
        self.loader.loaded_snapshot_id = hash_to_bytes(
            "a23699280a82a043f8c0994cf1631b568f716f95"
        )

        res = self.loader.load()
        assert res == {"status": "failed"}

        assert_last_visit_matches(
            self.loader.storage,
            self.repo_url,
            status="partial",
            type="git",
            snapshot=None,
        )


class FullGitLoaderTests(CommonGitLoaderTests):
    """Tests for GitLoader (from disk or not). Includes the common ones, and
       add others that only work with a local dir.

    """

    def test_load_changed(self):
        """Loads a repository, makes some changes by adding files, commits,
        and merges, load it again, and check the storage contains everything
        it should."""
        # Initial load
        res = self.loader.load()
        assert res == {"status": "eventful"}

        stats0 = get_stats(self.loader.storage)
        assert stats0 == {
            "content": 4,
            "directory": 7,
            "origin": 1,
            "origin_visit": 1,
            "release": 0,
            "revision": 7,
            "skipped_content": 0,
            "snapshot": 1,
        }

        # Load with a new file + revision
        with open(os.path.join(self.destination_path, "hello.py"), "a") as fd:
            fd.write("print('Hello world')\n")

        self.repo.stage([b"hello.py"])
        new_revision = self.repo.do_commit(b"Hello world\n").decode()
        new_dir = "85dae072a5aa9923ffa7a7568f819ff21bf49858"

        assert self.repo[new_revision.encode()].tree == new_dir.encode()

        revisions = REVISIONS1.copy()
        assert new_revision not in revisions
        revisions[new_revision] = new_dir

        res = self.loader.load()
        assert res == {"status": "eventful"}

        stats1 = get_stats(self.loader.storage)
        expected_stats = copy.deepcopy(stats0)
        # did one new visit
        expected_stats["origin_visit"] += 1
        # with one more of the following objects
        expected_stats["content"] += 1
        expected_stats["directory"] += 1
        expected_stats["revision"] += 1
        # concluding into 1 new snapshot
        expected_stats["snapshot"] += 1

        assert stats1 == expected_stats

        visit_status = assert_last_visit_matches(
            self.loader.storage, self.repo_url, status="full", type="git"
        )
        assert visit_status.snapshot is not None

        snapshot_id = visit_status.snapshot
        snapshot = snapshot_get_all_branches(self.loader.storage, snapshot_id)
        branches = snapshot.branches
        assert branches[b"HEAD"] == SnapshotBranch(
            target=b"refs/heads/master", target_type=TargetType.ALIAS,
        )
        assert branches[b"refs/heads/master"] == SnapshotBranch(
            target=hash_to_bytes(new_revision), target_type=TargetType.REVISION,
        )

        # Merge branch1 into HEAD.

        current = self.repo[b"HEAD"]
        branch1 = self.repo[b"refs/heads/branch1"]

        merged_tree = dulwich.objects.Tree()
        for item in self.repo[current.tree].items():
            merged_tree.add(*item)
        for item in self.repo[branch1.tree].items():
            merged_tree.add(*item)

        merged_dir_id = "dab8a37df8db8666d4e277bef9a546f585b5bedd"
        assert merged_tree.id.decode() == merged_dir_id
        self.repo.object_store.add_object(merged_tree)

        merge_commit = self.repo.do_commit(
            b"merge.\n", tree=merged_tree.id, merge_heads=[branch1.id]
        )

        assert merge_commit.decode() not in revisions
        revisions[merge_commit.decode()] = merged_tree.id.decode()

        res = self.loader.load()
        assert res == {"status": "eventful"}

        stats2 = get_stats(self.loader.storage)
        expected_stats = copy.deepcopy(stats1)
        # one more visit
        expected_stats["origin_visit"] += 1
        # with 1 new directory and revision
        expected_stats["directory"] += 1
        expected_stats["revision"] += 1
        # concluding into 1 new snapshot
        expected_stats["snapshot"] += 1

        assert stats2 == expected_stats

        visit_status = assert_last_visit_matches(
            self.loader.storage, self.repo_url, status="full", type="git"
        )
        assert visit_status.snapshot is not None

        merge_snapshot_id = visit_status.snapshot
        assert merge_snapshot_id != snapshot_id

        merge_snapshot = snapshot_get_all_branches(
            self.loader.storage, merge_snapshot_id
        )
        merge_branches = merge_snapshot.branches
        assert merge_branches[b"HEAD"] == SnapshotBranch(
            target=b"refs/heads/master", target_type=TargetType.ALIAS,
        )
        assert merge_branches[b"refs/heads/master"] == SnapshotBranch(
            target=hash_to_bytes(merge_commit.decode()),
            target_type=TargetType.REVISION,
        )

    def test_load_filter_branches(self):
        filtered_branches = {b"refs/pull/42/merge"}
        unfiltered_branches = {b"refs/pull/42/head"}

        # Add branches to the repository on disk; some should be filtered by
        # the loader, some should not.
        for branch_name in filtered_branches | unfiltered_branches:
            self.repo[branch_name] = self.repo[b"refs/heads/master"]

        # Generate the expected snapshot from SNAPSHOT1 (which is the original
        # state of the git repo)...
        branches = dict(SNAPSHOT1.branches)

        # ... and the unfiltered_branches, which are all pointing to the same
        # commit as "refs/heads/master".
        for branch_name in unfiltered_branches:
            branches[branch_name] = branches[b"refs/heads/master"]

        expected_snapshot = Snapshot(branches=branches)

        # Load the modified repository
        res = self.loader.load()
        assert res == {"status": "eventful"}

        check_snapshot(expected_snapshot, self.loader.storage)
        assert_last_visit_matches(
            self.loader.storage,
            self.repo_url,
            status="full",
            type="git",
            snapshot=expected_snapshot.id,
        )

    def test_load_dangling_symref(self):
        with open(os.path.join(self.destination_path, ".git/HEAD"), "wb") as f:
            f.write(b"ref: refs/heads/dangling-branch\n")

        res = self.loader.load()
        assert res == {"status": "eventful"}

        visit_status = assert_last_visit_matches(
            self.loader.storage, self.repo_url, status="full", type="git"
        )
        snapshot_id = visit_status.snapshot
        assert snapshot_id is not None

        snapshot = snapshot_get_all_branches(self.loader.storage, snapshot_id)
        branches = snapshot.branches

        assert branches[b"HEAD"] == SnapshotBranch(
            target=b"refs/heads/dangling-branch", target_type=TargetType.ALIAS,
        )
        assert branches[b"refs/heads/dangling-branch"] is None

        stats = get_stats(self.loader.storage)
        assert stats == {
            "content": 4,
            "directory": 7,
            "origin": 1,
            "origin_visit": 1,
            "release": 0,
            "revision": 7,
            "skipped_content": 0,
            "snapshot": 1,
        }

    def test_load_empty_tree(self):
        empty_dir_id = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

        # Check the empty tree does not already exist for some reason
        # (it would make this test pointless)
        assert list(
            self.loader.storage.directory_missing([hash_to_bytes(empty_dir_id)])
        ) == [hash_to_bytes(empty_dir_id)]

        empty_tree = dulwich.objects.Tree()
        assert empty_tree.id.decode() == empty_dir_id
        self.repo.object_store.add_object(empty_tree)

        self.repo.do_commit(b"remove all bugs\n", tree=empty_tree.id)

        res = self.loader.load()
        assert res == {"status": "eventful"}

        assert (
            list(self.loader.storage.directory_missing([hash_to_bytes(empty_dir_id)]))
            == []
        )
        results = self.loader.storage.directory_get_entries(hash_to_bytes(empty_dir_id))
        assert results.next_page_token is None
        assert results.results == []

    def test_load_tag(self):
        with open(os.path.join(self.destination_path, "hello.py"), "a") as fd:
            fd.write("print('Hello world')\n")

        self.repo.stage([b"hello.py"])
        new_revision = self.repo.do_commit(b"Hello world\n")

        # Newer Dulwich versions always add a \n to tag messages.
        if dulwich.__version__ >= (0, 20, 22):
            message = b"First release!"
        else:
            message = b"First release!\n"

        dulwich.porcelain.tag_create(
            self.repo,
            b"v1.0.0",
            message=message,
            annotated=True,
            objectish=new_revision,
        )

        res = self.loader.load()
        assert res == {"status": "eventful"}

        branches = self.loader.storage.snapshot_get_branches(self.loader.snapshot.id)

        branch = branches["branches"][b"refs/tags/v1.0.0"]
        assert branch.target_type == TargetType.RELEASE

        release = self.loader.storage.release_get([branch.target])[0]
        assert release.date is not None
        assert release.author is not None
        assert release == Release(
            name=b"v1.0.0",
            message=b"First release!\n",
            target_type=ObjectType.REVISION,
            target=bytehex_to_hash(new_revision),
            author=release.author,
            date=release.date,
            synthetic=False,
        )

    def test_load_tag_minimal(self):
        with open(os.path.join(self.destination_path, "hello.py"), "a") as fd:
            fd.write("print('Hello world')\n")

        self.repo.stage([b"hello.py"])
        new_revision = self.repo.do_commit(b"Hello world\n")

        # dulwich.porcelain.tag_create doesn't allow creating tags without
        # a tagger or a date, so we have to create it "manually"
        tag = dulwich.objects.Tag()
        tag.message = b"First release!\n"
        tag.name = b"v1.0.0"
        tag.object = (dulwich.objects.Commit, new_revision)
        self.repo.object_store.add_object(tag)
        self.repo[b"refs/tags/v1.0.0"] = tag.id

        res = self.loader.load()
        assert res == {"status": "eventful"}

        branches = self.loader.storage.snapshot_get_branches(self.loader.snapshot.id)

        print(list(branches["branches"]))
        branch = branches["branches"][b"refs/tags/v1.0.0"]
        assert branch.target_type == TargetType.RELEASE

        release = self.loader.storage.release_get([branch.target])[0]
        assert release == Release(
            id=bytehex_to_hash(tag.id),
            name=b"v1.0.0",
            message=b"First release!\n",
            target_type=ObjectType.REVISION,
            target=bytehex_to_hash(new_revision),
            synthetic=False,
        )


class TestGitLoaderFromDisk(FullGitLoaderTests):
    """Prepare a git directory repository to be loaded through a GitLoaderFromDisk.
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
        self.loader = GitLoaderFromDisk(
            swh_storage,
            url=self.repo_url,
            visit_date=datetime.datetime(
                2016, 5, 3, 15, 16, 32, tzinfo=datetime.timezone.utc
            ),
            directory=self.destination_path,
        )
        self.repo = dulwich.repo.Repo(self.destination_path)


class TestGitLoaderFromArchive(CommonGitLoaderTests):
    """Tests for GitLoaderFromArchive. Only tests common scenario."""

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path):
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        self.repo_url = archive_path
        self.loader = GitLoaderFromArchive(
            swh_storage,
            url=self.repo_url,
            archive_path=archive_path,
            visit_date=datetime.datetime(
                2016, 5, 3, 15, 16, 32, tzinfo=datetime.timezone.utc
            ),
        )
