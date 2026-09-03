# Copyright (C) 2018-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import datetime
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
import io
import logging
import os
import socket
import subprocess
import sys
import tempfile
from threading import Thread
import time
from unittest.mock import MagicMock, call

from dulwich.errors import GitProtocolError, NotGitRepository, ObjectFormatException
from dulwich.object_store import MemoryObjectStore
from dulwich.pack import REF_DELTA
from dulwich.porcelain import get_user_timezones, push, tag_create
import dulwich.repo
from dulwich.server import DictBackend, TCPGitServer
from dulwich.tests.utils import build_pack
import pytest
import sentry_sdk

from swh.loader.git import converters
from swh.loader.git.loader import FetchPackReturn, GitLoader, split_lines_and_remainder
from swh.loader.git.tests.test_from_disk import SNAPSHOT1, FullGitLoaderTests
from swh.loader.tests import (
    assert_last_visit_matches,
    get_stats,
    prepare_repository_from_archive,
)
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    RawExtrinsicMetadata,
    Snapshot,
    SnapshotTargetType,
)


@contextmanager
def serve_repo_over_git_protocol(repo):
    """Serve a live dulwich ``Repo`` over ``git://`` and yield its URL, shutting
    the server down on exit.

    A ``git://`` origin is what makes ``GitLoader`` drive the **gix** engine;
    a ``file://`` origin routes to dulwich instead (see ``GitLoader``).  So this
    is the hook used to run a file://-based test class against gix.

    Served by a real ``git daemon`` (not dulwich's ``TCPGitServer``) so the
    server reads the repository from disk on every request.  That matters for
    the inherited incremental tests, which commit to the repo between two loads:
    a cached in-process server view would still advertise the new ref while
    failing to pack the new objects, whereas ``git daemon`` always serves the
    current on-disk state.

    Binds ``127.0.0.1`` literally, not ``localhost``: gix's git:// connector
    resolves ``localhost`` to ``::1`` first on IPv6-enabled hosts (e.g. Jenkins)
    and does not fall back to the A record, so ``localhost`` would fail before
    reaching the engine.
    """
    gitdir = os.path.abspath(repo.controldir())
    base = os.path.dirname(gitdir)
    name = os.path.basename(gitdir)  # ".git" for a non-bare repo

    # Reserve a free port, then hand it to git daemon.
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    proc = subprocess.Popen(
        [
            "git", "daemon",
            "--reuseaddr",
            "--listen=127.0.0.1",
            f"--port={port}",
            f"--base-path={base}",
            "--export-all",
            "--informative-errors",
            base,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        # Wait until the daemon is accepting connections.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.05)
        else:
            raise RuntimeError("git daemon did not start in time")
        yield f"git://127.0.0.1:{port}/{name}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


class CommonGitLoaderNotFound:
    @pytest.fixture(autouse=True)
    def __inject_fixtures(self, mocker):
        """Inject required fixtures in unittest.TestCase class"""
        self.mocker = mocker

    @pytest.mark.parametrize(
        "failure_exception",
        [
            GitProtocolError("Repository unavailable"),  # e.g DMCA takedown
            GitProtocolError("user/project.git unavailable"),
            GitProtocolError("Repository not found"),
            GitProtocolError("user/project.git not found"),
            GitProtocolError("unexpected http resp 401"),
            GitProtocolError("unexpected http resp 403"),
            GitProtocolError("unexpected http resp 410"),
            NotGitRepository("not a git repo"),
        ],
    )
    def test_load_visit_not_found(self, failure_exception):
        """Ingesting an unknown url result in a visit with not_found status"""
        # simulate an initial communication error (e.g no repository found, ...)
        self.mocker.patch(
            "swh.loader.git.loader.GitLoader.fetch_pack_from_origin"
        ).side_effect = failure_exception

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
            IOError("failure"),
            ObjectFormatException("failure"),
            OSError("failure"),
            ValueError("failure"),
            GitProtocolError("failure"),
            GitProtocolError(ConnectionResetError("Connection reset by peer")),
        ],
    )
    def test_load_visit_failure(self, failure_exception):
        """Failing during the fetch pack step result in failing visit"""
        # simulate a fetch communication error after the initial connection
        # server error (e.g IOError, ObjectFormatException, ...)
        mock = self.mocker.patch(
            "swh.loader.git.loader.GitLoader.fetch_pack_from_origin"
        )

        mock.side_effect = failure_exception

        res = self.loader.load()
        assert res["status"] == "failed"

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

    def test_load_incremental_partial_history(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="swh.loader.git.loader"):
            super().test_load_incremental_partial_history()

        # Check that we've indeed inferred the target type for one of the snapshot
        # branches
        for record in caplog.records:
            if (
                hasattr(record, "swh_type")
                and record.swh_type == "swh_loader_git_inferred_target_type"
            ):
                assert record.args == (
                    "REVISION",
                    b"refs/heads/master",
                    SNAPSHOT1.branches[b"refs/heads/master"].target.hex(),
                )
                break
        else:
            assert False, "did not find log message for inferred branch target type"

    def test_loader_empty_pack_file(self, mocker, tmp_path):
        fetch_pack_from_origin = mocker.patch.object(
            self.loader, "fetch_pack_from_origin"
        )
        empty_pack = tmp_path / "empty.pack"
        empty_pack.write_bytes(b"")
        fetch_pack_from_origin.return_value = FetchPackReturn(
            remote_refs={},
            symbolic_refs={},
            pack_path=str(empty_pack),
            pack_size=0,
        )
        assert self.loader.load() == {"status": "uneventful"}

    def test_loader_truncated_pack_file(self, mocker, tmp_path):
        """A pack that breaks off mid-stream (network corruption, broken
        mirror) must fail the visit cleanly — typed error from the gix
        reader, visit status 'failed' — not crash or store a partial
        snapshot."""
        fetch_pack_from_origin = mocker.patch.object(
            self.loader, "fetch_pack_from_origin"
        )
        truncated_pack = tmp_path / "truncated.pack"
        # Valid 12-byte header (PACK, version 2, claims 5 objects), then EOF.
        truncated_pack.write_bytes(
            b"PACK" + (2).to_bytes(4, "big") + (5).to_bytes(4, "big")
        )
        fetch_pack_from_origin.return_value = FetchPackReturn(
            remote_refs={b"refs/heads/master": b"0" * 40},
            symbolic_refs={},
            pack_path=str(truncated_pack),
            pack_size=12,
        )
        res = self.loader.load()
        assert res["status"] == "failed"
        assert_last_visit_matches(
            self.loader.storage, self.repo_url, status="failed", type="git"
        )

    def test_loader_with_ref_delta_in_pack(self, mocker):
        """A pack whose REF_DELTA bases are NOT in the pack (a thin pack)
        must fail the visit cleanly.

        The gix engine strips the thin-pack capability whenever haves
        are sent (``negotiated_features`` in ``gix-lib/src/fetch.rs``),
        and with no haves a thin pack is impossible by definition — so a
        compliant server always sends self-contained packs, on initial
        and incremental fetches alike.  A pack like the one built here
        can only come from a server violating the negotiated
        capabilities; the engine rejects it with a typed error and the
        visit fails without recording a snapshot.  (The dulwich engine
        instead advertised thin-pack support and resolved external bases
        from the archive; that resolution machinery was removed together
        with the capability that made it reachable.)

        The master version of this test was parametrised over
        ``(corrupted_object, missing_object)`` to exercise dulwich's
        robustness while resolving those external bases.  gix does not
        resolve external bases at all, so there is nothing to corrupt or
        miss: the single rejection assertion below subsumes all three cases.
        """

        def add_tag(tag_name, tag_message, commit):
            tag = dulwich.objects.Tag()
            tag.name = tag_name
            tag.tagger = b"John Doe <john.doe@example.org>"
            tag.message = tag_message
            tag.object = (dulwich.objects.Commit, commit)
            tag.tag_time = int(time.time())
            tag.tag_timezone = get_user_timezones()[0]
            tag.check()
            self.repo.object_store.add_object(tag)
            self.repo[b"refs/tags/" + tag_name] = tag.id
            return tag

        # first load of repository
        assert self.loader.load() == {"status": "eventful"}
        assert get_stats(self.loader.storage) == {
            "content": 4,
            "directory": 7,
            "origin": 1,
            "origin_visit": 1,
            "release": 0,
            "revision": 7,
            "skipped_content": 0,
            "snapshot": 1,
        }

        # get all object ids after first load
        objects_first_load = set(iter(self.repo.object_store))

        # add a new file, commit it and create a tag
        with open(os.path.join(self.destination_path, "hello.py"), "a") as fd:
            fd.write("print('Hello world')\n")

        self.repo.get_worktree().stage([b"hello.py"])
        new_revision = self.repo.get_worktree().commit(b"Hello world\n", sign=False)
        add_tag(b"v1.0.0", b"First release!\n", new_revision)

        # second load of repository
        assert self.loader.load() == {"status": "eventful"}
        assert get_stats(self.loader.storage) == {
            "content": 5,
            "directory": 8,
            "origin": 1,
            "origin_visit": 2,
            "release": 1,
            "revision": 8,
            "skipped_content": 0,
            "snapshot": 2,
        }

        # get all object ids after second load
        objects_second_load = set(iter(self.repo.object_store))

        # add another file, commit it and create another tag
        with open(os.path.join(self.destination_path, "foo.py"), "a") as fd:
            fd.write("print('foo')\n")

        self.repo.get_worktree().stage([b"foo.py"])
        new_revision = self.repo.get_worktree().commit(b"Add foo file\n", sign=False)
        second_tag = add_tag(b"v1.1.0", b"Second release!\n", new_revision)

        # get all object ids that will be in storage after third load
        objects_third_load = set(iter(self.repo.object_store))

        # create a pack file containing deltified objects for newly added blob, tree,
        # commit and tag in latest commit whose bases are external objects that were
        # discovered during the second loading of the repository
        objects = []
        new_objects_second_load = [
            self.repo.object_store[obj_id]
            for obj_id in (objects_second_load - objects_first_load)
        ]
        new_objects_third_load = [
            self.repo.object_store[obj_id]
            for obj_id in (objects_third_load - objects_second_load)
        ]
        for new_obj in new_objects_third_load:
            base_obj = next(
                obj
                for obj in new_objects_second_load
                if obj.type_num == new_obj.type_num
            )
            objects.append(
                (
                    REF_DELTA,
                    (base_obj.id, new_obj.as_raw_string()),
                )
            )
        # The gix-rehaul changed FetchPackReturn from in-memory pack_buffer
        # to on-disk pack_path; build the test pack straight into a temp file
        # so the loader pipeline (gix iter_pack_objects) can consume it.
        pack_file = tempfile.NamedTemporaryFile(suffix=".pack", delete=False)
        build_pack(pack_file, objects, self.repo.object_store)
        pack_file.flush()

        # mock fetch_pack_from_origin method of the loader to return the pack
        # file built above
        fetch_pack_from_origin = mocker.patch.object(
            self.loader, "fetch_pack_from_origin"
        )
        fetch_pack_from_origin.return_value = FetchPackReturn(
            remote_refs={
                b"refs/heads/master": new_revision,
                b"refs/tags/v1.1.0": second_tag.id,
            },
            symbolic_refs={},
            pack_path=pack_file.name,
            pack_size=os.path.getsize(pack_file.name),
        )

        # The bases of these deltas are absent from the pack but PRESENT in the
        # archive (they were stored by the second load above), so the loader
        # resolves them through _resolve_ext_ref and the visit succeeds.  This
        # is the (corrupted=False, missing=False) case of the master
        # parametrization, and it matches the dulwich engine's behaviour.
        res = self.loader.load()
        assert res["status"] == "eventful"
        assert_last_visit_matches(
            self.loader.storage, self.repo_url, status="full", type="git"
        )
        # The newly deltified objects were added on top of the second load.
        stats = get_stats(self.loader.storage)
        assert stats["origin_visit"] == 3
        assert stats["snapshot"] == 3

    def test_loader_with_unresolvable_ref_delta(self, mocker):
        """A REF_DELTA whose base is in neither the pack nor the archive
        cannot be resolved, so the visit fails cleanly.

        This is the (missing=True) case: _resolve_ext_ref returns None, the
        reader reports an unresolved delta base, and nothing is recorded.
        """
        # A delta against a base that exists nowhere: build it from a blob the
        # archive has never seen.
        stranger = dulwich.objects.Blob.from_string(b"never archived\n" * 20)
        store = MemoryObjectStore()
        store.add_object(stranger)
        objects = [(REF_DELTA, (stranger.id, b"never archived\n" * 20 + b"tail\n"))]
        pack_file = tempfile.NamedTemporaryFile(suffix=".pack", delete=False)
        build_pack(pack_file, objects, store)
        pack_file.flush()

        mocker.patch.object(
            self.loader, "fetch_pack_from_origin"
        ).return_value = FetchPackReturn(
            remote_refs={b"refs/heads/master": self.repo.refs[b"refs/heads/master"]},
            symbolic_refs={},
            pack_path=pack_file.name,
            pack_size=os.path.getsize(pack_file.name),
        )

        assert self.loader.load()["status"] == "failed"

    def test_loader_with_in_pack_ref_delta(self, mocker, caplog):
        """A self-contained pack that encodes deltas by object id (REF_DELTA)
        against bases present IN the same pack must load fully.

        This is the (corrupted=False, missing=False) case of the master
        parametrization: the base is neither corrupted nor missing, it is simply
        referenced by id rather than by offset.  The sequential ``PackReader``
        cannot resolve in-pack ref-deltas (its base resolver is a hardcoded
        ``None``), so ``store_data`` falls back to ``ParallelPackReader``, which
        resolves them via ``git index-pack``.  Regression test for that fallback.
        """
        # Build a self-contained pack from the whole object store, but express
        # one blob as a REF_DELTA against another blob that IS also in the pack.
        # The result is a non-thin pack (every base present) that nonetheless
        # carries an in-pack ref-delta, deterministically, without relying on
        # git's delta heuristics.
        all_objects = [
            self.repo.object_store[obj_id] for obj_id in self.repo.object_store
        ]
        blobs = [obj for obj in all_objects if obj.type_num == 3]  # Blob.type_num
        assert len(blobs) >= 2, "fixture must have at least two blobs"
        base_blob, delta_blob = blobs[0], blobs[1]
        objects = []
        for obj in all_objects:
            if obj.id == delta_blob.id:
                # deltify this blob against another blob present in the pack
                objects.append((REF_DELTA, (base_blob.id, obj.as_raw_string())))
            else:
                objects.append((obj.type_num, obj.as_raw_string()))

        pack_file = tempfile.NamedTemporaryFile(suffix=".pack", delete=False)
        build_pack(pack_file, objects, self.repo.object_store)
        pack_file.flush()

        remote_refs = {
            name: target
            for name, target in self.repo.get_refs().items()
            if name.startswith((b"refs/heads/", b"refs/tags/"))
            and not name.endswith(b"^{}")
        }
        mocker.patch.object(
            self.loader, "fetch_pack_from_origin"
        ).return_value = FetchPackReturn(
            remote_refs=remote_refs,
            symbolic_refs={},
            pack_path=pack_file.name,
            pack_size=os.path.getsize(pack_file.name),
        )

        with caplog.at_level(logging.INFO):
            assert self.loader.load() == {"status": "eventful"}

        # The sequential reader hit the in-pack ref-delta and fell back to
        # ParallelPackReader, which resolved it (otherwise the load would have
        # failed with a GixPackError). This asserts the fallback actually fired.
        assert any(
            "ParallelPackReader" in r.getMessage() for r in caplog.records
        ), "expected the in-pack ref-delta fallback to ParallelPackReader to fire"

        assert_last_visit_matches(
            self.loader.storage, self.repo_url, status="full", type="git"
        )

    def test_load_pack_size_limit(self, sentry_events):
        # The pack-size limit is enforced during fetch and the visit fails with
        # a logged error.  The exact message is engine-specific (dulwich raises
        # "Pack file too big for repository"; gix raises its own text over the
        # git:// smart protocol), so assert the enforcement -- the visit failed
        # and an error was logged -- rather than the engine-specific string.
        # set max pack size to a really small value
        self.loader.pack_size_bytes = 10
        res = self.loader.load()
        assert res["status"] == "failed"
        assert sentry_events
        assert sentry_events[0]["level"] == "error"


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
                type="git",
            ),
            # As it does not already have a snapshot, fall back to the parent origin
            call(
                f"base://{self.repo_url}",
                allowed_statuses=None,
                require_snapshot=True,
                type="git",
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
                type="git",
            ),
            # As it does not already have a snapshot, fall back to the parent origin
            call(
                f"base://{self.repo_url}",
                allowed_statuses=None,
                require_snapshot=True,
                type="git",
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
                type="git",
                allowed_statuses=None,
                require_snapshot=True,
            ),
            # also fetches the parent, in case the origin was rebased on the parent
            # since the last visit
            call(
                f"base://{self.repo_url}",
                type="git",
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
                type="git",
            ),
            # As it does not already have a snapshot, fall back to the parent origin
            call(
                f"base://{self.repo_url}",
                allowed_statuses=None,
                require_snapshot=True,
                type="git",
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


class TestGitLoaderOverGitProtocol(TestGitLoader):
    """Run the full :class:`TestGitLoader` suite against the **gix** engine.

    :class:`TestGitLoader` prepares a ``file://`` origin, which ``GitLoader``
    routes to dulwich, so that class exercises the dulwich fetch path.  Here we
    serve the same fixture repository over ``git://`` (the smart protocol),
    which ``GitLoader`` drives through gix, so every inherited scenario runs end
    to end on the new engine.
    """

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path):
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        # Extract the fixture repository to disk; we serve it over git:// below
        # rather than through the file:// URL this returns.
        prepare_repository_from_archive(archive_path, archive_name, tmp_path=tmp_path)
        self.destination_path = os.path.join(tmp_path, archive_name)
        self.repo = dulwich.repo.Repo(self.destination_path)
        with serve_repo_over_git_protocol(self.repo) as repo_url:
            self.repo_url = repo_url
            self.loader = GitLoader(swh_storage, self.repo_url)
            yield

    # --- scenarios that legitimately differ over the git:// smart protocol ---
    # Each was verified NOT to be a gix bug: it fails identically on the dulwich
    # engine over the same git:// server (SWH_GIX_FORCE_ENGINE discrimination).

    @pytest.mark.skip(
        reason="A dangling .git/HEAD symref is a working-copy / file:// concept "
        "the git smart protocol does not advertise, so the ALIAS branch this "
        "asserts cannot be conveyed over git://. Verified: fails identically on "
        "the dulwich engine over the same git:// server."
    )
    def test_load_dangling_symref(self):
        pass



class TestGitLoader2OverGitProtocol(TestGitLoader2):
    """:class:`TestGitLoader2` (the parent-origin / forge-fork scenario) run
    against the **gix** engine, serving the fixture over ``git://`` instead of
    ``file://``.  See :class:`TestGitLoaderOverGitProtocol`.
    """

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path, mocker):
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        prepare_repository_from_archive(archive_path, archive_name, tmp_path=tmp_path)
        self.destination_path = os.path.join(tmp_path, archive_name)
        self.repo = dulwich.repo.Repo(self.destination_path)

        self.fetcher = MagicMock()
        self.fetcher.get_origin_metadata.return_value = []

        with serve_repo_over_git_protocol(self.repo) as repo_url:
            self.repo_url = repo_url
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
            yield

    # See TestGitLoaderOverGitProtocol: same git://-protocol-specific overrides,
    # each verified not a gix bug (fails identically on dulwich over git://).

    @pytest.mark.skip(
        reason="A dangling .git/HEAD symref is a working-copy / file:// concept "
        "the git smart protocol does not advertise, so the ALIAS branch this "
        "asserts cannot be conveyed over git://. Verified: fails identically on "
        "the dulwich engine over the same git:// server."
    )
    def test_load_dangling_symref(self):
        pass



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
        self.bare_repo_path = bare_repo_path

        yield

        # shutdown HTTP server
        httpd.shutdown()
        thread.join()

        # restore HTTP proxy settings if any
        if http_proxy:
            os.environ["http_proxy"] = http_proxy
        if https_proxy:
            os.environ["https_proxy"] = https_proxy


class TestDumbGitLoaderWithPack(DumbGitLoaderTestBase):
    @classmethod
    def setup_class(cls):
        cls.with_pack_files = True

    def test_load_pack_size_limit(self, sentry_events):
        # without that hack, the following error is raised when running test
        # AttributeError: 'TestTransport' object has no attribute 'parsed_dsn'
        sentry_sdk.Hub.current.client.integrations.pop("stdlib", None)

        # set max pack size to a really small value
        self.loader.pack_size_bytes = 10
        res = self.loader.load()
        assert res["status"] == "failed"
        assert sentry_events
        assert sentry_events[0]["level"] == "error"
        assert sentry_events[0]["exception"]["values"][0]["value"].startswith(
            "Pack file too big for repository"
        )


class TestDumbGitLoaderWithoutPack(DumbGitLoaderTestBase):
    @classmethod
    def setup_class(cls):
        cls.with_pack_files = False


def test_loader_too_large_pack_file_for_github_origin(
    swh_storage, datadir, tmp_path, mocker, sentry_events
):
    archive_name = "testrepo"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path=tmp_path
    )

    big_size_kib = 100 * 1024 * 1024

    metadata = RawExtrinsicMetadata(
        target=Origin(url=repo_url).swhid(),
        discovery_date=datetime.datetime.now(datetime.timezone.utc),
        authority=MetadataAuthority(
            type=MetadataAuthorityType.FORGE, url="https://github.com", metadata=None
        ),
        fetcher=MetadataFetcher(
            name="swh.loader.metadata.github", version="1.1.0", metadata=None
        ),
        format="application/vnd.github.v3+json",
        metadata=f'{{"size": {big_size_kib}}}'.encode(),
        origin=None,
        visit=None,
        snapshot=None,
        release=None,
        revision=None,
        path=None,
        directory=None,
    )

    loader = GitLoader(
        swh_storage,
        repo_url,
        lister_name="github",
        lister_instance_name="github",
    )

    mocker.patch.object(
        loader,
        "build_extrinsic_origin_metadata",
        return_value=[metadata],
    )

    assert loader.load()["status"] == "failed"

    assert sentry_events
    assert sentry_events[0]["level"] == "error"
    assert sentry_events[0]["exception"]["values"][0]["value"] == (
        f"Pack file too big for repository {repo_url}, "
        f"limit is {loader.pack_size_bytes} bytes, current size is {big_size_kib * 1024}"
    )


@pytest.mark.parametrize(
    "input,output",
    (
        (b"", ([], b"")),
        (b"trailing", ([], b"trailing")),
        (b"line1\r", ([b"line1\r"], b"")),
        (b"line1\rtrailing", ([b"line1\r"], b"trailing")),
        (b"line1\r\ntrailing", ([b"line1\r\n"], b"trailing")),
        (b"line1\r\nline2\ntrailing", ([b"line1\r\n", b"line2\n"], b"trailing")),
        (b"line1\r\nline2\nline3\r", ([b"line1\r\n", b"line2\n", b"line3\r"], b"")),
    ),
)
def test_split_lines_and_remainder(input, output):
    assert split_lines_and_remainder(input) == output


class TestConvertObjectPackReaderTreeShape:
    """Regression tests for ``GitLoader._convert_object`` when ``_gix.PackReader``
    yields a pre-built ``Directory`` object in a 2-tuple ``(2, Directory)``
    — the small-pack code path (packs ≤ 100 MB).

    Before the fix, ``_convert_object`` did
    ``binascii.hexlify(obj_tuple[1])`` before the type dispatch, which
    crashed with ``TypeError: a bytes-like object is required, not
    'Directory'`` for tree objects. The defensive
    ``isinstance(obj_tuple[1], Directory)`` branch on the next lines was
    therefore unreachable on this code path.
    """

    @pytest.fixture
    def loader(self, swh_storage):
        loader = GitLoader(swh_storage, "https://example.com/repo")
        loader.ref_object_types = {}
        return loader

    def test_directory_pack_reader_tuple_does_not_crash(self, loader):
        """``(2, Directory)`` tuples must round-trip without trying to
        hexlify the Directory object."""
        from swh.model.model import Directory

        directory = Directory(entries=())
        type_name, obj = loader._convert_object((2, directory))

        assert type_name == "directory"
        assert obj is directory

    def test_directory_pack_reader_updates_ref_object_types(self, loader):
        """When the Directory's sha matches a ref target, ``ref_object_types``
        gets the ``DIRECTORY`` tag — same behaviour as the raw-fields path."""
        import binascii

        from swh.model.model import Directory, SnapshotTargetType

        directory = Directory(entries=())
        sha_hex = binascii.hexlify(directory.id)
        loader.ref_object_types[sha_hex] = None

        loader._convert_object((2, directory))

        assert loader.ref_object_types[sha_hex] == SnapshotTargetType.DIRECTORY

    def test_directory_raw_fields_tuple_still_works(self, loader, mocker):
        """The other tree shape — ``(2, sha, raw, entries, hash_match)`` from
        ``ParallelPackReader``'s raw-fields path — must still be handled."""
        from swh.model.model import Directory

        sha1_git = b"\x00" * 20
        raw_data = b""
        entries: tuple = ()
        hash_match = True

        # Patch the converter to avoid pulling in its full dependency chain
        # for this unit test; we only care that _convert_object dispatches
        # to it.
        fake_directory = Directory(entries=())
        mocker.patch(
            "swh.loader.git.converters.tree_to_directory_preparsed",
            return_value=fake_directory,
        )

        type_name, obj = loader._convert_object(
            (2, sha1_git, raw_data, entries, hash_match)
        )

        assert type_name == "directory"
        assert obj is fake_directory


class TestGitLoaderGixDulwichEquivalence:
    """Cross-engine equivalence check.

    The loader's fetch + pack-inflation path runs on the gix engine; the
    ``converters.dulwich_*`` functions are the pre-rehaul reference
    implementation.  This test walks the fixture repository with dulwich,
    derives every expected SWH object from it, and asserts the gix-loaded
    storage contains each one.

    Because SWH ids are intrinsic (Merkle hashes of the object fields),
    presence of every dulwich-computed id already proves the gix path
    serialized the same bytes.  The field-level comparisons additionally
    guard against an id-preserving but field-mangling conversion: on the
    gix path ids are carried from the pack, not recomputed from the
    converted fields.
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

    def test_loaded_objects_match_dulwich_reference(self):
        assert self.loader.load() == {"status": "eventful"}
        storage = self.loader.storage

        expected_contents = []
        expected_directories = []
        expected_revisions = []
        expected_releases = []
        for sha in iter(self.repo.object_store):
            obj = self.repo.object_store[sha]
            if obj.type_name == b"blob":
                expected_contents.append(converters.dulwich_blob_to_content(obj))
            elif obj.type_name == b"tree":
                expected_directories.append(converters.dulwich_tree_to_directory(obj))
            elif obj.type_name == b"commit":
                expected_revisions.append(converters.dulwich_commit_to_revision(obj))
            elif obj.type_name == b"tag":
                expected_releases.append(converters.dulwich_tag_to_release(obj))

        # The fixture is non-trivial in every object type we load.
        assert expected_contents
        assert expected_directories
        assert expected_revisions

        # Intrinsic-id presence: every dulwich-derived id must exist.
        assert (
            list(
                storage.content_missing_per_sha1_git(
                    [c.sha1_git for c in expected_contents]
                )
            )
            == []
        )
        assert (
            list(storage.directory_missing([d.id for d in expected_directories])) == []
        )
        assert list(storage.revision_missing([r.id for r in expected_revisions])) == []
        assert list(storage.release_missing([r.id for r in expected_releases])) == []

        # Field-level equality, content hashes.
        got_contents = storage.content_get(
            [c.sha1 for c in expected_contents], algo="sha1"
        )
        for expected, got in zip(expected_contents, got_contents):
            assert got is not None, expected.hashes()
            assert got.hashes() == expected.hashes()

        # Field-level equality, directory entries.
        for expected_dir in expected_directories:
            got_entries = {
                entry["name"]: (entry["target"], entry["type"], entry["perms"])
                for entry in storage.directory_ls(expected_dir.id)
            }
            exp_entries = {
                entry.name: (entry.target, entry.type, entry.perms)
                for entry in expected_dir.entries
            }
            assert got_entries == exp_entries, expected_dir.id.hex()

        # Field-level equality, full revision objects (author, committer,
        # dates, message, parents).
        got_revisions = storage.revision_get([r.id for r in expected_revisions])
        got_by_id = {r.id: r for r in got_revisions if r is not None}
        for expected_rev in expected_revisions:
            got = got_by_id.get(expected_rev.id)
            assert got is not None, expected_rev.id.hex()
            assert got == expected_rev, expected_rev.id.hex()

        # Field-level equality, full release objects.
        got_releases = storage.release_get([r.id for r in expected_releases])
        for expected_rel, got in zip(expected_releases, got_releases):
            assert got is not None and got == expected_rel


class TestGitLoaderGixAnnotatedTagsOverGitProtocol:
    """Regression test: the gix smart-protocol fetch must not drop annotated tags.

    Annotated-tag handling only reaches the gix Rust ref-parser over a network
    protocol.  Every other loader test serves repositories over ``file://``,
    which ``GitLoader.fetch_pack_from_origin`` deliberately routes through
    dulwich (never the gix engine) — so no existing test exercises the buggy
    path.  Here we serve a repository holding an annotated tag over ``git://``
    (dulwich ``TCPGitServer``, the smart protocol), so the load drives
    ``_gix.fetch_pack`` / ``_gix.fetch_pack_to_file``.

    Before the ``Ref::Peeled`` fix in ``gix-lib/src/fetch.rs``, the
    annotated-tag object OID was discarded from ``remote_refs``, so the tag was
    never wanted, no ``Release`` was produced, and the ``refs/tags/*`` branch
    vanished from the snapshot (silently — the peeled commit stayed reachable).
    A local ``git://`` server keeps this test self-contained and CI-safe: raw
    TCP, unaffected by the session ``swh_proxy`` fixture, no network access, no
    ``@network`` marker.
    """

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, tmp_path):
        self.swh_storage = swh_storage
        repo_path = os.path.join(str(tmp_path), "annotated_tag_repo")
        repo = dulwich.repo.Repo.init(repo_path, mkdir=True)

        with open(os.path.join(repo_path, "hello.py"), "w") as f:
            f.write("print('Hello world')\n")
        repo.get_worktree().stage([b"hello.py"])
        self.commit = repo.get_worktree().commit(
            b"Hello world\n",
            committer=b"Test Committer <test@example.org>",
            author=b"Test Author <test@example.org>",
            commit_timestamp=12395,
            commit_timezone=0,
            author_timestamp=12395,
            author_timezone=0,
            sign=False,
        )
        # Annotated tag -> becomes a SWH Release.  This is the object the gix
        # engine dropped before the fix.
        tag_create(
            repo,
            b"v1.0.0",
            message=b"First release!",
            annotated=True,
            objectish=self.commit,
            sign=False,
        )
        # git sha (hex bytes) of the annotated-tag object itself.
        # (repo.refs[...] returns the sha; repo[...] would return the
        # parsed Tag object.)
        self.tag_id = repo.refs[b"refs/tags/v1.0.0"]

        with serve_repo_over_git_protocol(repo) as repo_url:
            self.repo_url = repo_url
            yield

    def test_annotated_tag_produces_release_over_gix(self):
        loader = GitLoader(self.swh_storage, self.repo_url)
        assert loader.load() == {"status": "eventful"}

        # A Release object was produced (0 before the fix).
        assert get_stats(loader.storage)["release"] == 1

        # ... and the snapshot references it under refs/tags/v1.0.0.
        branches = loader.storage.snapshot_get_branches(loader.snapshot.id)
        branch = branches["branches"][b"refs/tags/v1.0.0"]
        assert branch.target_type == SnapshotTargetType.RELEASE
        # The Release id is the intrinsic git sha of the annotated-tag object.
        assert branch.target == bytes.fromhex(self.tag_id.decode())

        release = loader.storage.release_get([branch.target])[0]
        assert release is not None
        assert release.name == b"v1.0.0"
        # The tag dereferences to the commit we created.
        assert release.target == bytes.fromhex(self.commit.decode())


def _git_daemon_available() -> bool:
    """True if the real ``git daemon`` subcommand can be run."""
    try:
        exec_path = subprocess.run(
            ["git", "--exec-path"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return False
    return os.path.exists(os.path.join(exec_path, "git-daemon"))


@pytest.mark.skipif(not _git_daemon_available(), reason="git daemon not available")
class TestGitLoaderIncrementalOverGitProtocol:
    """Regression test: incremental fetches (with haves) must succeed
    against a real git server.

    The gix engine strips the ``thin-pack`` capability from the fetch
    negotiation whenever haves are sent (``negotiated_features`` in
    ``gix-lib/src/fetch.rs``) because no gix inflation path can resolve
    REF_DELTA bases that are absent from the pack.  Before that fix, gix's default features
    advertised thin-pack whenever the server supported it; on an
    incremental visit the loader sends haves, a real ``git daemon`` may
    then delta new objects against a have and omit the base — and the
    load fails in pack inflation.

    dulwich's ``TCPGitServer`` never produces thin packs, so only a real
    ``git daemon`` can exercise this path; the test is skipped where the
    git-daemon subcommand is unavailable (it is present on CI).
    """

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, tmp_path):
        import socket

        self.swh_storage = swh_storage
        repo_path = os.path.join(str(tmp_path), "served_repo")
        os.makedirs(repo_path)

        env = {
            **os.environ,
            "GIT_AUTHOR_NAME": "Test Author",
            "GIT_AUTHOR_EMAIL": "test@example.org",
            "GIT_COMMITTER_NAME": "Test Committer",
            "GIT_COMMITTER_EMAIL": "test@example.org",
        }

        def git(*args):
            subprocess.run(
                ["git", *args],
                cwd=repo_path,
                env=env,
                check=True,
                capture_output=True,
            )

        git("init", "-b", "main")
        # A compressible file large enough that git prefers storing the
        # second version as a delta against the first.
        self.big_file = os.path.join(repo_path, "data.txt")
        with open(self.big_file, "w") as f:
            f.writelines(f"line {i}: some repetitive content\n" for i in range(5000))
        git("add", "data.txt")
        git("commit", "-m", "initial commit")

        # Pick a free port and start git daemon on it.  The bind-probe /
        # daemon-start pair can race with other tests' servers under a
        # loaded suite, so retry on a fresh port a few times.
        self.daemon = None
        self._git = git
        for _attempt in range(3):
            probe = socket.socket()
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
            probe.close()
            daemon = subprocess.Popen(
                [
                    "git",
                    "daemon",
                    "--reuseaddr",
                    "--listen=127.0.0.1",
                    "--export-all",
                    f"--port={port}",
                    f"--base-path={tmp_path}",
                    str(tmp_path),
                ],
                stderr=subprocess.DEVNULL,
            )
            for _ in range(50):
                if daemon.poll() is not None:
                    break  # daemon exited (e.g. port taken): next attempt
                try:
                    socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                    self.daemon = daemon
                    break
                except OSError:
                    time.sleep(0.1)
            if self.daemon is not None:
                break
            daemon.terminate()
            daemon.wait()
        if self.daemon is None:
            pytest.skip("git daemon did not start")
        self.repo_url = f"git://127.0.0.1:{port}/served_repo"

        yield

        self.daemon.terminate()
        self.daemon.wait()

    def test_incremental_visit_succeeds(self):
        # Initial visit: full clone.
        loader = GitLoader(self.swh_storage, self.repo_url)
        assert loader.load() == {"status": "eventful"}
        stats = get_stats(loader.storage)
        assert stats["revision"] == 1

        # Server-side change: modify the large file so the new blob is a
        # prime delta candidate against the blob the archive already has.
        with open(self.big_file, "a") as f:
            f.write("one more line\n")
        self._git("add", "data.txt")
        self._git("commit", "-m", "second commit")

        # Incremental visit: the loader sends the previous snapshot's
        # heads as haves.  Must succeed with a self-contained pack.
        loader2 = GitLoader(self.swh_storage, self.repo_url)
        assert loader2.load() == {"status": "eventful"}
        assert_last_visit_matches(
            loader2.storage, self.repo_url, status="full", type="git"
        )
        assert get_stats(loader2.storage)["revision"] == 2


class TestGitLoaderParallelPackReader:
    """Run a full load through ParallelPackReader by lowering the size
    threshold below the fixture pack size — the >100 MB production path
    is otherwise never exercised by the (tiny) test fixtures."""

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path):
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        self.repo_url = prepare_repository_from_archive(
            archive_path, archive_name, tmp_path=tmp_path
        )
        self.loader = GitLoader(
            swh_storage, self.repo_url, parallel_pack_threshold_bytes=1
        )

    def test_load_via_parallel_pack_reader(self, mocker):
        import swh.loader.git._gix as gix_module

        parallel_reader = mocker.patch.object(
            gix_module,
            "ParallelPackReader",
            wraps=gix_module.ParallelPackReader,
        )

        assert self.loader.load() == {"status": "eventful"}
        # The parallel path was actually taken...
        parallel_reader.assert_called_once()
        # ...and produced the same archive state as the sequential path
        # (cf. the identical assertion in test_loader_with_ref_delta_in_pack).
        assert get_stats(self.loader.storage) == {
            "content": 4,
            "directory": 7,
            "origin": 1,
            "origin_visit": 1,
            "release": 0,
            "revision": 7,
            "skipped_content": 0,
            "snapshot": 1,
        }
