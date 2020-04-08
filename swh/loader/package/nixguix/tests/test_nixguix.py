# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
from json.decoder import JSONDecodeError

from swh.loader.package.nixguix.loader import (
    NixGuixLoader,
    retrieve_sources,
    clean_sources,
)

from swh.loader.package.tests.common import get_stats, check_snapshot

sources_url = "https://nix-community.github.io/nixpkgs-swh/sources.json"


def test_retrieve_sources(swh_config, requests_mock_datadir):
    j = retrieve_sources(sources_url)
    assert "sources" in j.keys()
    assert len(j["sources"]) == 2


def test_retrieve_non_existing(swh_config, requests_mock_datadir):
    with pytest.raises(ValueError):
        NixGuixLoader("https://non-existing-url")


def test_retrieve_non_json(swh_config, requests_mock_datadir):
    with pytest.raises(JSONDecodeError):
        NixGuixLoader("https://example.com/file.txt")


def test_clean_sources_invalid_schema(swh_config, requests_mock_datadir):
    sources = {}
    with pytest.raises(ValueError, match="sources structure invalid, missing: .*"):
        clean_sources(sources)


def test_clean_sources_invalid_version(swh_config, requests_mock_datadir):
    sources = {"version": 2, "sources": [], "revision": "my-revision"}

    with pytest.raises(
        ValueError, match="sources structure version .* is not supported"
    ):
        clean_sources(sources)


def test_clean_sources_invalid_sources(swh_config, requests_mock_datadir):
    sources = {
        "version": 1,
        "sources": [
            # Valid source
            {"type": "url", "urls": ["my-url"], "integrity": "my-integrity"},
            # integrity is missing
            {"type": "url", "urls": ["my-url"],},
            # urls is not a list
            {"type": "url", "urls": "my-url", "integrity": "my-integrity"},
            # type is not url
            {"type": "git", "urls": ["my-url"], "integrity": "my-integrity"},
        ],
        "revision": "my-revision",
    }
    clean = clean_sources(sources)

    assert len(clean["sources"]) == 1


def test_loader_one_visit(swh_config, requests_mock_datadir):
    loader = NixGuixLoader(sources_url)
    res = loader.load()
    assert res["status"] == "eventful"

    stats = get_stats(loader.storage)
    assert {
        "content": 1,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "person": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 0,
        "snapshot": 1,
    } == stats

    origin_visit = loader.storage.origin_visit_get_latest(sources_url)
    # The visit is partial because urls pointing to non tarball file
    # are not handled yet
    assert origin_visit["status"] == "partial"
    assert origin_visit["type"] == "nixguix"


def test_uncompress_failure(swh_config, requests_mock_datadir):
    """Non tarball files are currently not supported and the uncompress
    function fails on such kind of files.

    However, even in this case of failure (because of the url
    https://example.com/file.txt), a snapshot and a visit has to be
    created (with a status partial since all files are not archived).

    """
    loader = NixGuixLoader(sources_url)
    loader_status = loader.load()

    urls = [s["urls"][0] for s in loader.sources]
    assert "https://example.com/file.txt" in urls
    assert loader_status["status"] == "eventful"

    origin_visit = loader.storage.origin_visit_get_latest(sources_url)
    # The visit is partial because urls pointing to non tarball files
    # are not handled yet
    assert origin_visit["status"] == "partial"


def test_loader_incremental(swh_config, requests_mock_datadir):
    """Ensure a second visit do not download artifact already
    downloaded by the previous visit.

    """
    loader = NixGuixLoader(sources_url)
    load_status = loader.load()

    loader.load()
    expected_snapshot_id = "0c5881c74283793ebe9a09a105a9381e41380383"
    assert load_status == {"status": "eventful", "snapshot_id": expected_snapshot_id}
    expected_branches = {
        "evaluation": {
            "target": "cc4e04c26672dd74e5fd0fecb78b435fb55368f7",
            "target_type": "revision",
        },
        "https://github.com/owner-1/repository-1/revision-1.tgz": {
            "target": "488ad4e7b8e2511258725063cf43a2b897c503b4",
            "target_type": "revision",
        },
    }
    expected_snapshot = {
        "id": expected_snapshot_id,
        "branches": expected_branches,
    }
    check_snapshot(expected_snapshot, storage=loader.storage)

    urls = [
        m.url
        for m in requests_mock_datadir.request_history
        if m.url == ("https://github.com/owner-1/repository-1/revision-1.tgz")
    ]
    # The artifact
    # 'https://github.com/owner-1/repository-1/revision-1.tgz' is only
    # visited one time
    assert len(urls) == 1


def test_loader_two_visits(swh_config, requests_mock_datadir_visits):
    """To ensure there is only one origin, but two visits, two revisions
    and two snapshots are created.

    The first visit creates a snapshot containing one tarball. The
    second visit creates a snapshot containing the same tarball and
    another tarball.

    """
    loader = NixGuixLoader(sources_url)
    load_status = loader.load()
    expected_snapshot_id = "0c5881c74283793ebe9a09a105a9381e41380383"
    assert load_status == {"status": "eventful", "snapshot_id": expected_snapshot_id}

    expected_branches = {
        "evaluation": {
            "target": "cc4e04c26672dd74e5fd0fecb78b435fb55368f7",
            "target_type": "revision",
        },
        "https://github.com/owner-1/repository-1/revision-1.tgz": {
            "target": "488ad4e7b8e2511258725063cf43a2b897c503b4",
            "target_type": "revision",
        },
    }

    expected_snapshot = {
        "id": expected_snapshot_id,
        "branches": expected_branches,
    }
    check_snapshot(expected_snapshot, storage=loader.storage)

    stats = get_stats(loader.storage)
    assert {
        "content": 1,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "person": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 0,
        "snapshot": 1,
    } == stats

    loader = NixGuixLoader(sources_url)
    load_status = loader.load()
    expected_snapshot_id = "b0bfa75cbd0cc90aac3b9e95fb0f59c731176d97"
    assert load_status == {"status": "eventful", "snapshot_id": expected_snapshot_id}

    # This ensures visits are incremental. Indeed, if we request a
    # second time an url, because of the requests_mock_datadir_visits
    # fixture, the file has to end with `_visit1`.
    expected_branches = {
        "evaluation": {
            "target": "602140776b2ce6c9159bcf52ada73a297c063d5e",
            "target_type": "revision",
        },
        "https://github.com/owner-1/repository-1/revision-1.tgz": {
            "target": "488ad4e7b8e2511258725063cf43a2b897c503b4",
            "target_type": "revision",
        },
        "https://github.com/owner-2/repository-1/revision-1.tgz": {
            "target": "85e0bad74e33e390aaeb74f139853ae3863ee544",
            "target_type": "revision",
        },
    }

    expected_snapshot = {
        "id": expected_snapshot_id,
        "branches": expected_branches,
    }
    check_snapshot(expected_snapshot, storage=loader.storage)

    stats = get_stats(loader.storage)
    assert {
        "content": 2,
        "directory": 5,
        "origin": 1,
        "origin_visit": 2,
        "person": 1,
        "release": 0,
        "revision": 2,
        "skipped_content": 0,
        "snapshot": 2,
    } == stats


def test_resolve_revision_from(swh_config, requests_mock_datadir):
    loader = NixGuixLoader(sources_url)

    known_artifacts = {
        "id1": {"extrinsic": {"raw": {"url": "url1", "integrity": "integrity1"}}},
        "id2": {"extrinsic": {"raw": {"url": "url2", "integrity": "integrity2"}}},
    }

    metadata = {"url": "url1", "integrity": "integrity1"}
    assert loader.resolve_revision_from(known_artifacts, metadata) == "id1"
    metadata = {"url": "url3", "integrity": "integrity3"}
    assert loader.resolve_revision_from(known_artifacts, metadata) == None  # noqa


def test_evaluation_branch(swh_config, requests_mock_datadir):
    loader = NixGuixLoader(sources_url)
    res = loader.load()
    assert res["status"] == "eventful"

    expected_branches = {
        "https://github.com/owner-1/repository-1/revision-1.tgz": {
            "target": "488ad4e7b8e2511258725063cf43a2b897c503b4",
            "target_type": "revision",
        },
        "evaluation": {
            "target": "cc4e04c26672dd74e5fd0fecb78b435fb55368f7",
            "target_type": "revision",
        },
    }

    expected_snapshot = {
        "id": "0c5881c74283793ebe9a09a105a9381e41380383",
        "branches": expected_branches,
    }

    check_snapshot(expected_snapshot, storage=loader.storage)


def test_eoferror(swh_config, requests_mock_datadir):
    """Load a truncated archive which is invalid to make the uncompress
    function raising the exception EOFError. We then check if a
    snapshot is created, meaning this error is well managed.

    """
    sources = (
        "https://nix-community.github.io/nixpkgs-swh/sources-EOFError.json"  # noqa
    )
    loader = NixGuixLoader(sources)
    loader.load()

    expected_branches = {
        "evaluation": {
            "target": "cc4e04c26672dd74e5fd0fecb78b435fb55368f7",
            "target_type": "revision",
        },
    }
    expected_snapshot = {
        "id": "4257fa2350168c6bfec726a06452ea27a2c0cb33",
        "branches": expected_branches,
    }

    check_snapshot(expected_snapshot, storage=loader.storage)
