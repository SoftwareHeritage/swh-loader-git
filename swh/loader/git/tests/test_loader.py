# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
import subprocess
import sys
from tempfile import SpooledTemporaryFile
from threading import Thread
from unittest.mock import MagicMock, call

from dulwich.errors import GitProtocolError, NotGitRepository, ObjectFormatException
from dulwich.porcelain import push
import dulwich.repo
import pytest

from swh.loader.git import converters, dumb
from swh.loader.git.loader import GitLoader
from swh.loader.git.tests.test_from_disk import SNAPSHOT1, FullGitLoaderTests
from swh.loader.tests import (
    assert_last_visit_matches,
    get_stats,
    prepare_repository_from_archive,
)
from swh.model.model import Origin, OriginVisit, OriginVisitStatus, Snapshot


class CommonGitLoaderNotFound:
    @pytest.fixture(autouse=True)
    def __inject_fixtures(self, mocker):
        """Inject required fixtures in unittest.TestCase class"""
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
        """Ingesting an unknown url result in a visit with not_found status"""
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
        [
            IOError,
            ObjectFormatException,
            OSError,
            ValueError,
            GitProtocolError,
        ],
    )
    def test_load_visit_failure(self, failure_exception):
        """Failing during the fetch pack step result in failing visit"""
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

    def test_metrics(self, mocker):
        statsd_report = mocker.patch.object(self.loader.statsd, "_report")
        res = self.loader.load()
        assert res == {"status": "eventful"}

        # TODO: assert "incremental" is added to constant tags before these
        # metrics are sent
        statsd_calls = statsd_report.mock_calls
        assert [c for c in statsd_calls if c[1][0].startswith("git_")] == [
            call("git_total", "c", 1, {}, 1),
            call("git_ignored_refs_percent", "h", 0.0, {}, 1),
            call("git_known_refs_percent", "h", 0.0, {}, 1),
        ]
        total_sum_name = "filtered_objects_total_sum"
        total_count_name = "filtered_objects_total_count"
        percent_name = "filtered_objects_percent"
        assert [c for c in statsd_calls if c[1][0].startswith("filtered_")] == [
            call(percent_name, "h", 0.0, {"object_type": "content"}, 1),
            call(total_sum_name, "c", 0, {"object_type": "content"}, 1),
            call(total_count_name, "c", 4, {"object_type": "content"}, 1),
            call(percent_name, "h", 0.0, {"object_type": "directory"}, 1),
            call(total_sum_name, "c", 0, {"object_type": "directory"}, 1),
            call(total_count_name, "c", 7, {"object_type": "directory"}, 1),
            call(percent_name, "h", 0.0, {"object_type": "revision"}, 1),
            call(total_sum_name, "c", 0, {"object_type": "revision"}, 1),
            call(total_count_name, "c", 7, {"object_type": "revision"}, 1),
            call(percent_name, "h", 0.0, {"object_type": "snapshot"}, 1),
            call(total_sum_name, "c", 0, {"object_type": "snapshot"}, 1),
            call(total_count_name, "c", 1, {"object_type": "snapshot"}, 1),
        ]
        assert self.loader.statsd.constant_tags == {
            "visit_type": "git",
            "incremental_enabled": True,
            "has_parent_snapshot": False,
            "has_previous_snapshot": False,
            "has_parent_origins": False,
        }

    def test_metrics_filtered(self, mocker):
        """Tests that presence of some objects in the storage (but not referenced
        by a snapshot) is reported"""

        known_revs = [
            converters.dulwich_commit_to_revision(self.repo[sha1])
            for sha1 in [
                b"b6f40292c4e94a8f7e7b4aff50e6c7429ab98e2a",
                b"1135e94ccf73b5f9bd6ef07b3fa2c5cc60bba69b",
            ]
        ]
        known_dirs = [
            converters.dulwich_tree_to_directory(self.repo[sha1])
            for sha1 in [
                b"fbf70528223d263661b5ad4b80f26caf3860eb8e",
                b"9ca0c7d6ffa3f9f0de59fd7912e08f11308a1338",
                b"5df34ec74d6f69072d9a0a6677d8efbed9b12e60",
            ]
        ]
        known_cnts = [
            converters.dulwich_blob_to_content(self.repo[sha1])
            for sha1 in [
                b"534d61ecee4f6da4d6ca6ddd8abf258208d2d1bc",
            ]
        ]
        self.loader.storage.revision_add(known_revs)
        self.loader.storage.directory_add(known_dirs)
        self.loader.storage.content_add(known_cnts)
        self.loader.storage.flush()

        statsd_report = mocker.patch.object(self.loader.statsd, "_report")
        res = self.loader.load()
        assert res == {"status": "eventful"}

        # TODO: assert "incremental" is added to constant tags before these
        # metrics are sent
        statsd_calls = statsd_report.mock_calls
        assert [c for c in statsd_calls if c[1][0].startswith("git_")] == [
            call("git_total", "c", 1, {}, 1),
            call("git_ignored_refs_percent", "h", 0.0, {}, 1),
            call("git_known_refs_percent", "h", 0.0, {}, 1),
        ]
        total_sum_name = "filtered_objects_total_sum"
        total_count_name = "filtered_objects_total_count"
        percent_name = "filtered_objects_percent"
        assert [c for c in statsd_calls if c[1][0].startswith("filtered_")] == [
            call(percent_name, "h", 1 / 4, {"object_type": "content"}, 1),
            call(total_sum_name, "c", 1, {"object_type": "content"}, 1),
            call(total_count_name, "c", 4, {"object_type": "content"}, 1),
            call(percent_name, "h", 3 / 7, {"object_type": "directory"}, 1),
            call(total_sum_name, "c", 3, {"object_type": "directory"}, 1),
            call(total_count_name, "c", 7, {"object_type": "directory"}, 1),
            call(percent_name, "h", 2 / 7, {"object_type": "revision"}, 1),
            call(total_sum_name, "c", 2, {"object_type": "revision"}, 1),
            call(total_count_name, "c", 7, {"object_type": "revision"}, 1),
            call(percent_name, "h", 0.0, {"object_type": "snapshot"}, 1),
            call(total_sum_name, "c", 0, {"object_type": "snapshot"}, 1),
            call(total_count_name, "c", 1, {"object_type": "snapshot"}, 1),
        ]
        assert self.loader.statsd.constant_tags == {
            "visit_type": "git",
            "incremental_enabled": True,
            "has_parent_snapshot": False,
            "has_previous_snapshot": False,
            "has_parent_origins": False,
        }


class TestGitLoader2(FullGitLoaderTests, CommonGitLoaderNotFound):
    """Mostly the same loading scenario but with a ``parent_origin`` different from the
    ``origin``; as if the ``origin`` was a forge-fork of ``parent_origin``, detected
    by the metadata loader.

    To walk slightly different paths, the end result should stay the same.

    """

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path, mocker):
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        self.repo_url = prepare_repository_from_archive(
            archive_path, archive_name, tmp_path=tmp_path
        )
        self.destination_path = os.path.join(tmp_path, archive_name)

        self.fetcher = MagicMock()
        self.fetcher.get_origin_metadata.return_value = []
        self.fetcher.get_parent_origins.return_value = [
            Origin(url=f"base://{self.repo_url}")
        ]
        self.fetcher_cls = MagicMock(return_value=self.fetcher)
        self.fetcher_cls.SUPPORTED_LISTERS = ["fake-lister"]
        mocker.patch(
            "swh.loader.core.metadata_fetchers._fetchers",
            return_value=[self.fetcher_cls],
        )

        self.loader = GitLoader(
            MagicMock(wraps=swh_storage),
            self.repo_url,
            lister_name="fake-lister",
            lister_instance_name="",
        )
        self.repo = dulwich.repo.Repo(self.destination_path)

    def test_no_previous_snapshot(self, mocker):
        statsd_report = mocker.patch.object(self.loader.statsd, "_report")
        res = self.loader.load()
        assert res == {"status": "eventful"}

        self.fetcher_cls.assert_called_once_with(
            credentials={},
            lister_name="fake-lister",
            lister_instance_name="",
            origin=Origin(url=self.repo_url),
        )
        self.fetcher.get_parent_origins.assert_called_once_with()

        # First tries the same origin
        assert self.loader.storage.origin_visit_get_latest.mock_calls == [
            call(
                self.repo_url,
                allowed_statuses=None,
                require_snapshot=True,
                type=None,
            ),
            # As it does not already have a snapshot, fall back to the parent origin
            call(
                f"base://{self.repo_url}",
                allowed_statuses=None,
                require_snapshot=True,
                type=None,
            ),
        ]

        # TODO: assert "incremental" is added to constant tags before these
        # metrics are sent
        assert [c for c in statsd_report.mock_calls if c[1][0].startswith("git_")] == [
            call("git_total", "c", 1, {}, 1),
            call("git_ignored_refs_percent", "h", 0.0, {}, 1),
            call("git_known_refs_percent", "h", 0.0, {}, 1),
        ]
        assert self.loader.statsd.constant_tags == {
            "visit_type": "git",
            "incremental_enabled": True,
            "has_parent_snapshot": False,
            "has_previous_snapshot": False,
            "has_parent_origins": True,
        }

    def test_load_incremental(self, mocker):
        statsd_report = mocker.patch.object(self.loader.statsd, "_report")

        snapshot_id = b"\x01" * 20
        now = datetime.datetime.now(tz=datetime.timezone.utc)

        def ovgl(origin_url, allowed_statuses, require_snapshot, type):
            if origin_url == f"base://{self.repo_url}":
                return OriginVisit(origin=origin_url, visit=42, date=now, type="git")
            else:
                return None

        self.loader.storage.origin_visit_get_latest.side_effect = ovgl
        self.loader.storage.origin_visit_status_get_latest.return_value = (
            OriginVisitStatus(
                origin=f"base://{self.repo_url}",
                visit=42,
                snapshot=snapshot_id,
                date=now,
                status="full",
            )
        )
        self.loader.storage.snapshot_get_branches.return_value = {
            "id": snapshot_id,
            "branches": {
                b"refs/heads/master": SNAPSHOT1.branches[b"refs/heads/master"]
            },
            "next_branch": None,
        }

        res = self.loader.load()
        assert res == {"status": "eventful"}

        self.fetcher_cls.assert_called_once_with(
            credentials={},
            lister_name="fake-lister",
            lister_instance_name="",
            origin=Origin(url=self.repo_url),
        )
        self.fetcher.get_parent_origins.assert_called_once_with()

        # First tries the same origin
        assert self.loader.storage.origin_visit_get_latest.mock_calls == [
            call(
                self.repo_url,
                allowed_statuses=None,
                require_snapshot=True,
                type=None,
            ),
            # As it does not already have a snapshot, fall back to the parent origin
            call(
                f"base://{self.repo_url}",
                allowed_statuses=None,
                require_snapshot=True,
                type=None,
            ),
        ]

        # TODO: assert "incremental*" is added to constant tags before these
        # metrics are sent
        assert [c for c in statsd_report.mock_calls if c[1][0].startswith("git_")] == [
            call("git_total", "c", 1, {}, 1),
            call("git_ignored_refs_percent", "h", 0.0, {}, 1),
            call("git_known_refs_percent", "h", 0.25, {}, 1),
        ]
        assert self.loader.statsd.constant_tags == {
            "visit_type": "git",
            "incremental_enabled": True,
            "has_parent_snapshot": True,
            "has_previous_snapshot": False,
            "has_parent_origins": True,
        }

        self.fetcher.reset_mock()
        self.fetcher_cls.reset_mock()
        if sys.version_info >= (3, 9, 0):
            self.loader.storage.reset_mock(return_value=True, side_effect=True)
        else:
            # Reimplement https://github.com/python/cpython/commit/aef7dc89879d099dc704bd8037b8a7686fb72838  # noqa
            # for old Python versions:
            def reset_mock(m):
                m.reset_mock(return_value=True, side_effect=True)
                for child in m._mock_children.values():
                    reset_mock(child)

            reset_mock(self.loader.storage)
        statsd_report.reset_mock()

        # Load again
        res = self.loader.load()
        assert res == {"status": "uneventful"}

        self.fetcher_cls.assert_called_once_with(
            credentials={},
            lister_name="fake-lister",
            lister_instance_name="",
            origin=Origin(url=self.repo_url),
        )
        self.fetcher.get_parent_origins.assert_not_called()

        assert self.loader.storage.origin_visit_get_latest.mock_calls == [
            # Tries the same origin, and finds a snapshot
            call(
                self.repo_url,
                type=None,
                allowed_statuses=None,
                require_snapshot=True,
            ),
            # also fetches the parent, in case the origin was rebased on the parent
            # since the last visit
            call(
                f"base://{self.repo_url}",
                type=None,
                allowed_statuses=None,
                require_snapshot=True,
            ),
        ]

        # TODO: assert "incremental*" is added to constant tags before these
        # metrics are sent
        assert [c for c in statsd_report.mock_calls if c[1][0].startswith("git_")] == [
            call("git_total", "c", 1, {}, 1),
            call("git_ignored_refs_percent", "h", 0.0, {}, 1),
            call("git_known_refs_percent", "h", 1.0, {}, 1),
        ]
        assert self.loader.statsd.constant_tags == {
            "visit_type": "git",
            "incremental_enabled": True,
            "has_parent_snapshot": False,  # Because we reset the mock since last time
            "has_previous_snapshot": True,
            "has_parent_origins": True,
        }

    @pytest.mark.parametrize(
        "parent_snapshot,previous_snapshot,expected_git_known_refs_percent",
        [
            pytest.param(
                Snapshot(
                    branches={
                        b"refs/heads/master": SNAPSHOT1.branches[b"refs/heads/master"]
                    }
                ),
                Snapshot(branches={}),
                0.25,
                id="partial-parent-and-empty-previous",
            ),
            pytest.param(
                SNAPSHOT1,
                Snapshot(
                    branches={
                        b"refs/heads/master": SNAPSHOT1.branches[b"refs/heads/master"]
                    }
                ),
                1.0,
                id="full-parent-and-partial-previous",
            ),
        ],
    )
    def test_load_incremental_from(
        self,
        parent_snapshot,
        previous_snapshot,
        expected_git_known_refs_percent,
        mocker,
    ):
        """Snapshot of parent origin has all branches, but previous snapshot was
        empty."""
        statsd_report = mocker.patch.object(self.loader.statsd, "_report")

        now = datetime.datetime.now(tz=datetime.timezone.utc)

        self.loader.storage.snapshot_add([parent_snapshot, previous_snapshot])
        self.loader.storage.origin_add(
            [Origin(url=f"base://{self.repo_url}"), Origin(url=self.repo_url)]
        )
        self.loader.storage.origin_visit_add(
            [
                OriginVisit(
                    origin=f"base://{self.repo_url}",
                    visit=42,
                    date=now - datetime.timedelta(seconds=-1),
                    type="git",
                ),
                OriginVisit(
                    origin=self.repo_url,
                    visit=42,
                    date=now - datetime.timedelta(seconds=-1),
                    type="git",
                ),
            ]
        )
        self.loader.storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=f"base://{self.repo_url}",
                    visit=42,
                    type="git",
                    snapshot=parent_snapshot.id,
                    date=now,
                    status="full",
                ),
                OriginVisitStatus(
                    origin=self.repo_url,
                    visit=42,
                    type="git",
                    snapshot=previous_snapshot.id,
                    date=now,
                    status="full",
                ),
            ]
        )
        self.loader.storage.flush()

        res = self.loader.load()
        assert res == {"status": "eventful"}

        self.fetcher_cls.assert_called_once_with(
            credentials={},
            lister_name="fake-lister",
            lister_instance_name="",
            origin=Origin(url=self.repo_url),
        )
        self.fetcher.get_parent_origins.assert_called_once_with()

        # First tries the same origin
        assert self.loader.storage.origin_visit_get_latest.mock_calls == [
            call(
                self.repo_url,
                allowed_statuses=None,
                require_snapshot=True,
                type=None,
            ),
            # As it does not already have a snapshot, fall back to the parent origin
            call(
                f"base://{self.repo_url}",
                allowed_statuses=None,
                require_snapshot=True,
                type=None,
            ),
        ]

        assert self.loader.statsd.constant_tags == {
            "visit_type": "git",
            "incremental_enabled": True,
            "has_parent_snapshot": True,
            "has_previous_snapshot": True,
            "has_parent_origins": True,
        }
        assert [c for c in statsd_report.mock_calls if c[1][0].startswith("git_")] == [
            call("git_total", "c", 1, {}, 1),
            call("git_ignored_refs_percent", "h", 0.0, {}, 1),
            call("git_known_refs_percent", "h", expected_git_known_refs_percent, {}, 1),
        ]


class DumbGitLoaderTestBase(FullGitLoaderTests):
    """Prepare a git repository to be loaded using the HTTP dumb transfer protocol."""

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path):
        # remove any proxy settings in order to successfully spawn a local HTTP server
        http_proxy = os.environ.get("http_proxy")
        https_proxy = os.environ.get("https_proxy")
        if http_proxy:
            del os.environ["http_proxy"]
        if http_proxy:
            del os.environ["https_proxy"]

        # prepare test base repository using smart transfer protocol
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        base_repo_url = prepare_repository_from_archive(
            archive_path, archive_name, tmp_path=tmp_path
        )
        destination_path = os.path.join(tmp_path, archive_name)
        self.destination_path = destination_path
        with_pack_files = self.with_pack_files

        if with_pack_files:
            # create a bare clone of that repository in another folder,
            # all objects will be contained in one or two pack files in that case
            http_root_dir = tmp_path
            repo_name = archive_name + "_bare"
            bare_repo_path = os.path.join(http_root_dir, repo_name)
            subprocess.run(
                ["git", "clone", "--bare", base_repo_url, bare_repo_path],
                check=True,
            )
        else:
            # otherwise serve objects from the bare repository located in
            # the .git folder of the base repository
            http_root_dir = destination_path
            repo_name = ".git"
            bare_repo_path = os.path.join(http_root_dir, repo_name)

        # spawn local HTTP server that will serve the bare repository files
        hostname = "localhost"
        handler = partial(SimpleHTTPRequestHandler, directory=http_root_dir)
        httpd = HTTPServer((hostname, 0), handler, bind_and_activate=True)

        def serve_forever(httpd):
            with httpd:
                httpd.serve_forever()

        thread = Thread(target=serve_forever, args=(httpd,))
        thread.start()

        repo = dulwich.repo.Repo(self.destination_path)

        class DumbGitLoaderTest(GitLoader):
            def load(self):
                """
                Override load method to ensure the bare repository will be synchronized
                with the base one as tests can modify its content.
                """
                if with_pack_files:
                    # ensure HEAD ref will be the same for both repositories
                    with open(os.path.join(bare_repo_path, "HEAD"), "wb") as fw:
                        with open(
                            os.path.join(destination_path, ".git/HEAD"), "rb"
                        ) as fr:
                            head_ref = fr.read()
                            fw.write(head_ref)

                    # push possibly modified refs in the base repository to the bare one
                    for ref in repo.refs.allkeys():
                        if ref != b"HEAD" or head_ref in repo.refs:
                            push(
                                repo,
                                remote_location=f"file://{bare_repo_path}",
                                refspecs=ref,
                            )

                # generate or update the info/refs file used in dumb protocol
                subprocess.run(
                    ["git", "-C", bare_repo_path, "update-server-info"],
                    check=True,
                )

                return super().load()

        # bare repository with dumb protocol only URL
        self.repo_url = f"http://{httpd.server_name}:{httpd.server_port}/{repo_name}"
        self.loader = DumbGitLoaderTest(swh_storage, self.repo_url)
        self.repo = repo

        yield

        # shutdown HTTP server
        httpd.shutdown()
        thread.join()

        # restore HTTP proxy settings if any
        if http_proxy:
            os.environ["http_proxy"] = http_proxy
        if https_proxy:
            os.environ["https_proxy"] = https_proxy

    @pytest.mark.parametrize(
        "failure_exception", [AttributeError, NotImplementedError, ValueError]
    )
    def test_load_despite_dulwich_exception(self, mocker, failure_exception):
        """Checks repository can still be loaded when dulwich raises exception
        when encountering a repository with dumb transfer protocol.
        """

        fetch_pack_from_origin = mocker.patch(
            "swh.loader.git.loader.GitLoader.fetch_pack_from_origin"
        )

        fetch_pack_from_origin.side_effect = failure_exception("failure")

        res = self.loader.load()

        assert res == {"status": "eventful"}

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

    def test_load_empty_repository(self, mocker):
        class GitObjectsFetcherNoRefs(dumb.GitObjectsFetcher):
            def _get_refs(self):
                return {}

        mocker.patch.object(dumb, "GitObjectsFetcher", GitObjectsFetcherNoRefs)

        res = self.loader.load()

        assert res == {"status": "uneventful"}

        stats = get_stats(self.loader.storage)
        assert stats == {
            "content": 0,
            "directory": 0,
            "origin": 1,
            "origin_visit": 1,
            "release": 0,
            "revision": 0,
            "skipped_content": 0,
            "snapshot": 1,
        }


class TestDumbGitLoaderWithPack(DumbGitLoaderTestBase):
    @classmethod
    def setup_class(cls):
        cls.with_pack_files = True

    def test_load_with_missing_pack(self, mocker):
        """Some dumb git servers might reference a no longer existing pack file
        while it is possible to load a repository without it.
        """

        class GitObjectsFetcherMissingPack(dumb.GitObjectsFetcher):
            def _http_get(self, path: str) -> SpooledTemporaryFile:
                buffer = super()._http_get(path)
                if path == "objects/info/packs":
                    # prepend a non existing pack to the returned packs list
                    packs = buffer.read().decode("utf-8")
                    buffer.seek(0)
                    buffer.write(
                        (
                            "P pack-a70762ba1a901af3a0e76de02fc3a99226842745.pack\n"
                            + packs
                        ).encode()
                    )
                    buffer.flush()
                    buffer.seek(0)
                return buffer

        mocker.patch.object(dumb, "GitObjectsFetcher", GitObjectsFetcherMissingPack)

        res = self.loader.load()

        assert res == {"status": "eventful"}


class TestDumbGitLoaderWithoutPack(DumbGitLoaderTestBase):
    @classmethod
    def setup_class(cls):
        cls.with_pack_files = False
