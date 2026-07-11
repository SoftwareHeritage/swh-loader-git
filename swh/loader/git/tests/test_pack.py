# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Tests for :mod:`swh.loader.git.pack` (save_pack / GitLoaderFromPack).

These cover the mass-ingestion consolidation entry points: fetching a
repository to a local pack (:func:`save_pack`) and ingesting that pack
offline (:class:`GitLoaderFromPack`), plus the :class:`PackRefs` serializer
and the forge-PR-ref filtering.
"""

import os

import pytest

from swh.loader.git.loader import GitLoader
from swh.loader.git.pack import (
    GitLoaderFromPack,
    PackRefs,
    _is_pull_ref,
    _PrDroppingRepoRepresentation,
    save_pack,
)
from swh.loader.tests import prepare_repository_from_archive
from swh.storage import get_storage
from swh.storage.algos.snapshot import snapshot_get_all_branches


def test_packrefs_roundtrip():
    """PackRefs survives a JSON round-trip, including non-UTF-8 refnames."""
    pr = PackRefs(
        remote_refs={b"refs/heads/m\xe9": b"a" * 40},
        symbolic_refs={b"HEAD": b"refs/heads/m\xe9"},
        origin_url="https://example.org/x.git",
    )
    back = PackRefs.from_json(pr.to_json())
    assert back.remote_refs == pr.remote_refs
    assert back.symbolic_refs == pr.symbolic_refs
    assert back.origin_url == pr.origin_url


def test_is_pull_ref():
    assert _is_pull_ref(b"refs/pull/1/head")
    assert _is_pull_ref(b"refs/merge-requests/2/head")
    assert not _is_pull_ref(b"refs/heads/main")
    assert not _is_pull_ref(b"refs/tags/v1")


def test_pr_dropping_determine_wants():
    """The PR-dropping base repo excludes pull/merge-request refs from wants."""
    storage = get_storage(cls="memory")
    base = _PrDroppingRepoRepresentation(storage, base_snapshots=[], incremental=False)
    refs = {
        b"refs/heads/main": b"1" * 40,
        b"refs/pull/1/head": b"2" * 40,
        b"refs/merge-requests/3/head": b"3" * 40,
    }
    wants = base.determine_wants(refs)
    assert b"1" * 40 in wants
    assert b"2" * 40 not in wants
    assert b"3" * 40 not in wants


class TestSavePackRoundTrip:
    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path):
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        self.repo_url = prepare_repository_from_archive(
            archive_path, archive_name, tmp_path=str(tmp_path)
        )
        self.tmp_path = str(tmp_path)

    def test_save_pack_writes_pack_and_refs(self):
        """save_pack writes a non-empty pack and captures the repo's refs."""
        dest = os.path.join(self.tmp_path, "out.pack")
        refs = save_pack(self.repo_url, dest)
        assert os.path.getsize(dest) > 0
        assert refs.origin_url == self.repo_url
        assert any(n.startswith(b"refs/heads/") for n in refs.remote_refs)

    def test_from_pack_matches_direct_load(self, swh_storage):
        """Ingesting the saved pack yields the same snapshot as a direct load.

        A direct GitLoader over the repository and a GitLoaderFromPack over the
        pack that save_pack produced must build the identical snapshot id — the
        pack + captured refs carry everything the snapshot needs.
        """
        # Reference: load the repository directly.
        ref_loader = GitLoader(get_storage(cls="memory"), self.repo_url)
        assert ref_loader.load()["status"] == "eventful"
        ref_snapshot = ref_loader.loaded_snapshot_id

        # Consolidation path: fetch to a pack, then ingest it offline.
        dest = os.path.join(self.tmp_path, "rt.pack")
        refs = save_pack(self.repo_url, dest)
        loader = GitLoaderFromPack(swh_storage, self.repo_url, dest, refs)
        assert loader.load()["status"] == "eventful"
        assert loader.loaded_snapshot_id == ref_snapshot

        # The snapshot is retrievable and has the expected HEAD alias.
        snapshot = snapshot_get_all_branches(swh_storage, loader.loaded_snapshot_id)
        assert snapshot is not None
        assert b"HEAD" in snapshot.branches
