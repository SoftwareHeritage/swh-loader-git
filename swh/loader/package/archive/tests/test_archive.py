# Copyright (C) 2019-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib
from io import BytesIO
from pathlib import Path
import string

import attr
import pytest
from requests.exceptions import ContentDecodingError

from swh.loader.package.archive.loader import ArchiveLoader, ArchivePackageInfo
from swh.loader.tests import assert_last_visit_matches, check_snapshot, get_stats
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Snapshot, SnapshotBranch, TargetType

URL = "https://ftp.gnu.org/gnu/8sync/"
GNU_ARTIFACTS = [
    {
        "time": 944729610,
        "url": "https://ftp.gnu.org/gnu/8sync/8sync-0.1.0.tar.gz",
        "length": 221837,
        "filename": "8sync-0.1.0.tar.gz",
        "version": "0.1.0",
    },
    {
        "time": 1480991830,
        "url": "https://ftp.gnu.org/gnu/8sync/8sync-0.2.0.tar.gz",
        "length": 238466,
        "filename": "8sync-0.2.0.tar.gz",
        "version": "0.2.0",
    },
]

_expected_new_contents_first_visit = [
    "e9258d81faf5881a2f96a77ba609396f82cb97ad",
    "1170cf105b04b7e2822a0e09d2acf71da7b9a130",
    "fbd27c3f41f2668624ffc80b7ba5db9b92ff27ac",
    "0057bec9b5422aff9256af240b177ac0e3ac2608",
    "2b8d0d0b43a1078fc708930c8ddc2956a86c566e",
    "27de3b3bc6545d2a797aeeb4657c0e215a0c2e55",
    "2e6db43f5cd764e677f416ff0d0c78c7a82ef19b",
    "ae9be03bd2a06ed8f4f118d3fe76330bb1d77f62",
    "edeb33282b2bffa0e608e9d2fd960fd08093c0ea",
    "d64e64d4c73679323f8d4cde2643331ba6c20af9",
    "7a756602914be889c0a2d3952c710144b3e64cb0",
    "84fb589b554fcb7f32b806951dcf19518d67b08f",
    "8624bcdae55baeef00cd11d5dfcfa60f68710a02",
    "e08441aeab02704cfbd435d6445f7c072f8f524e",
    "f67935bc3a83a67259cda4b2d43373bd56703844",
    "809788434b433eb2e3cfabd5d591c9a659d5e3d8",
    "7d7c6c8c5ebaeff879f61f37083a3854184f6c41",
    "b99fec102eb24bffd53ab61fc30d59e810f116a2",
    "7d149b28eaa228b3871c91f0d5a95a2fa7cb0c68",
    "f0c97052e567948adf03e641301e9983c478ccff",
    "7fb724242e2b62b85ca64190c31dcae5303e19b3",
    "4f9709e64a9134fe8aefb36fd827b84d8b617ab5",
    "7350628ccf194c2c3afba4ac588c33e3f3ac778d",
    "0bb892d9391aa706dc2c3b1906567df43cbe06a2",
    "49d4c0ce1a16601f1e265d446b6c5ea6b512f27c",
    "6b5cc594ac466351450f7f64a0b79fdaf4435ad3",
    "3046e5d1f70297e2a507b98224b6222c9688d610",
    "1572607d456d7f633bc6065a2b3048496d679a31",
]

_expected_new_directories_first_visit = [
    "daabc65ec75d487b1335ffc101c0ac11c803f8fc",
    "263be23b4a8101d3ad0d9831319a3e0f2b065f36",
    "7f6e63ba6eb3e2236f65892cd822041f1a01dd5c",
    "4db0a3ecbc976083e2dac01a62f93729698429a3",
    "dfef1c80e1098dd5deda664bb44a9ab1f738af13",
    "eca971d346ea54d95a6e19d5051f900237fafdaa",
    "3aebc29ed1fccc4a6f2f2010fb8e57882406b528",
]

_expected_new_revisions_first_visit = {
    "44183488c0774ce3c957fa19ba695cf18a4a42b3": (
        "3aebc29ed1fccc4a6f2f2010fb8e57882406b528"
    )
}


def test_archive_visit_with_no_artifact_found(swh_storage, requests_mock_datadir):
    url = URL
    unknown_artifact_url = "https://ftp.g.o/unknown/8sync-0.1.0.tar.gz"
    loader = ArchiveLoader(
        swh_storage,
        url,
        artifacts=[
            {
                "time": 944729610,
                "url": unknown_artifact_url,  # unknown artifact
                "length": 221837,
                "filename": "8sync-0.1.0.tar.gz",
                "version": "0.1.0",
            }
        ],
    )

    actual_load_status = loader.load()
    assert actual_load_status["status"] == "uneventful"
    assert actual_load_status["snapshot_id"] is not None
    stats = get_stats(swh_storage)

    assert {
        "content": 0,
        "directory": 0,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 1,
    } == stats

    assert_last_visit_matches(swh_storage, url, status="partial", type="tar")


def test_archive_visit_with_release_artifact_no_prior_visit(
    swh_storage, requests_mock_datadir
):
    """With no prior visit, load a gnu project ends up with 1 snapshot

    """
    loader = ArchiveLoader(swh_storage, URL, artifacts=GNU_ARTIFACTS[:1])

    actual_load_status = loader.load()
    assert actual_load_status["status"] == "eventful"

    expected_snapshot_first_visit_id = hash_to_bytes(
        "c419397fd912039825ebdbea378bc6283f006bf5"
    )

    assert (
        hash_to_bytes(actual_load_status["snapshot_id"])
        == expected_snapshot_first_visit_id
    )

    assert_last_visit_matches(swh_storage, URL, status="full", type="tar")

    stats = get_stats(swh_storage)
    assert {
        "content": len(_expected_new_contents_first_visit),
        "directory": len(_expected_new_directories_first_visit),
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": len(_expected_new_revisions_first_visit),
        "skipped_content": 0,
        "snapshot": 1,
    } == stats

    expected_contents = map(hash_to_bytes, _expected_new_contents_first_visit)
    assert list(swh_storage.content_missing_per_sha1(expected_contents)) == []

    expected_dirs = map(hash_to_bytes, _expected_new_directories_first_visit)
    assert list(swh_storage.directory_missing(expected_dirs)) == []

    expected_revs = map(hash_to_bytes, _expected_new_revisions_first_visit)
    assert list(swh_storage.revision_missing(expected_revs)) == []

    expected_snapshot = Snapshot(
        id=expected_snapshot_first_visit_id,
        branches={
            b"HEAD": SnapshotBranch(
                target_type=TargetType.ALIAS, target=b"releases/0.1.0",
            ),
            b"releases/0.1.0": SnapshotBranch(
                target_type=TargetType.REVISION,
                target=hash_to_bytes("44183488c0774ce3c957fa19ba695cf18a4a42b3"),
            ),
        },
    )

    check_snapshot(expected_snapshot, swh_storage)


def test_archive_2_visits_without_change(swh_storage, requests_mock_datadir):
    """With no prior visit, load a gnu project ends up with 1 snapshot

    """
    url = URL
    loader = ArchiveLoader(swh_storage, url, artifacts=GNU_ARTIFACTS[:1])

    actual_load_status = loader.load()
    assert actual_load_status["status"] == "eventful"
    assert actual_load_status["snapshot_id"] is not None

    assert_last_visit_matches(swh_storage, url, status="full", type="tar")

    actual_load_status2 = loader.load()
    assert actual_load_status2["status"] == "uneventful"
    assert actual_load_status2["snapshot_id"] is not None
    assert actual_load_status["snapshot_id"] == actual_load_status2["snapshot_id"]

    assert_last_visit_matches(swh_storage, url, status="full", type="tar")

    urls = [
        m.url
        for m in requests_mock_datadir.request_history
        if m.url.startswith("https://ftp.gnu.org")
    ]
    assert len(urls) == 1


def test_archive_2_visits_with_new_artifact(swh_storage, requests_mock_datadir):
    """With no prior visit, load a gnu project ends up with 1 snapshot

    """
    url = URL
    artifact1 = GNU_ARTIFACTS[0]
    loader = ArchiveLoader(swh_storage, url, [artifact1])

    actual_load_status = loader.load()
    assert actual_load_status["status"] == "eventful"
    assert actual_load_status["snapshot_id"] is not None

    assert_last_visit_matches(swh_storage, url, status="full", type="tar")

    stats = get_stats(swh_storage)
    assert {
        "content": len(_expected_new_contents_first_visit),
        "directory": len(_expected_new_directories_first_visit),
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": len(_expected_new_revisions_first_visit),
        "skipped_content": 0,
        "snapshot": 1,
    } == stats

    urls = [
        m.url
        for m in requests_mock_datadir.request_history
        if m.url.startswith("https://ftp.gnu.org")
    ]
    assert len(urls) == 1

    artifact2 = GNU_ARTIFACTS[1]

    loader2 = ArchiveLoader(swh_storage, url, [artifact1, artifact2])
    stats2 = get_stats(swh_storage)
    assert stats == stats2  # ensure we share the storage

    actual_load_status2 = loader2.load()
    assert actual_load_status2["status"] == "eventful"
    assert actual_load_status2["snapshot_id"] is not None

    stats2 = get_stats(swh_storage)
    assert {
        "content": len(_expected_new_contents_first_visit) + 14,
        "directory": len(_expected_new_directories_first_visit) + 8,
        "origin": 1,
        "origin_visit": 1 + 1,
        "release": 0,
        "revision": len(_expected_new_revisions_first_visit) + 1,
        "skipped_content": 0,
        "snapshot": 1 + 1,
    } == stats2

    assert_last_visit_matches(swh_storage, url, status="full", type="tar")

    urls = [
        m.url
        for m in requests_mock_datadir.request_history
        if m.url.startswith("https://ftp.gnu.org")
    ]
    # 1 artifact (2nd time no modification) + 1 new artifact
    assert len(urls) == 2


def test_archive_2_visits_without_change_not_gnu(swh_storage, requests_mock_datadir):
    """Load a project archive (not gnu) ends up with 1 snapshot

    """
    url = "https://something.else.org/8sync/"
    artifacts = [  # this is not a gnu artifact
        {
            "time": "1999-12-09T09:53:30+00:00",  # it's also not a timestamp
            "sha256": "d5d1051e59b2be6f065a9fc6aedd3a391e44d0274b78b9bb4e2b57a09134dbe4",  # noqa
            # keep a gnu artifact reference to avoid adding other test files
            "url": "https://ftp.gnu.org/gnu/8sync/8sync-0.2.0.tar.gz",
            "length": 238466,
            "filename": "8sync-0.2.0.tar.gz",
            "version": "0.2.0",
        }
    ]

    # Here the loader defines the id_keys to use for existence in the snapshot
    # It's not the default archive loader which
    loader = ArchiveLoader(
        swh_storage,
        url,
        artifacts=artifacts,
        extid_manifest_format="$sha256 $length $url",
    )

    actual_load_status = loader.load()
    assert actual_load_status["status"] == "eventful"
    assert actual_load_status["snapshot_id"] is not None
    assert_last_visit_matches(swh_storage, url, status="full", type="tar")

    actual_load_status2 = loader.load()
    assert actual_load_status2["status"] == "uneventful"
    assert actual_load_status2["snapshot_id"] == actual_load_status["snapshot_id"]
    assert_last_visit_matches(swh_storage, url, status="full", type="tar")

    urls = [
        m.url
        for m in requests_mock_datadir.request_history
        if m.url.startswith("https://ftp.gnu.org")
    ]
    assert len(urls) == 1


def test_archive_extid():
    """Compute primary key should return the right identity

    """

    @attr.s
    class TestPackageInfo(ArchivePackageInfo):
        a = attr.ib()
        b = attr.ib()

    metadata = GNU_ARTIFACTS[0]

    p_info = TestPackageInfo(
        raw_info={**metadata, "a": 1, "b": 2}, a=1, b=2, **metadata,
    )

    for manifest_format, expected_manifest in [
        (string.Template("$a $b"), b"1 2"),
        (string.Template(""), b""),
        (None, "{time} {length} {version} {url}".format(**metadata).encode()),
    ]:
        actual_id = p_info.extid(manifest_format=manifest_format)
        assert actual_id == (
            "package-manifest-sha256",
            hashlib.sha256(expected_manifest).digest(),
        )

    with pytest.raises(KeyError):
        p_info.extid(manifest_format=string.Template("$a $unknown_key"))


def test_archive_snapshot_append(swh_storage, requests_mock_datadir):
    # first loading with a first artifact
    artifact1 = GNU_ARTIFACTS[0]
    loader = ArchiveLoader(swh_storage, URL, [artifact1], snapshot_append=True)
    actual_load_status = loader.load()
    assert actual_load_status["status"] == "eventful"
    assert actual_load_status["snapshot_id"] is not None
    assert_last_visit_matches(swh_storage, URL, status="full", type="tar")

    # check expected snapshot
    snapshot = loader.last_snapshot()
    assert len(snapshot.branches) == 2
    branch_artifact1_name = f"releases/{artifact1['version']}".encode()
    assert b"HEAD" in snapshot.branches
    assert branch_artifact1_name in snapshot.branches
    assert snapshot.branches[b"HEAD"].target == branch_artifact1_name

    # second loading with a second artifact
    artifact2 = GNU_ARTIFACTS[1]
    loader = ArchiveLoader(swh_storage, URL, [artifact2], snapshot_append=True)
    actual_load_status = loader.load()
    assert actual_load_status["status"] == "eventful"
    assert actual_load_status["snapshot_id"] is not None
    assert_last_visit_matches(swh_storage, URL, status="full", type="tar")

    # check expected snapshot, should contain a new branch and the
    # branch for the first artifact
    snapshot = loader.last_snapshot()
    assert len(snapshot.branches) == 3
    branch_artifact2_name = f"releases/{artifact2['version']}".encode()
    assert b"HEAD" in snapshot.branches
    assert branch_artifact2_name in snapshot.branches
    assert branch_artifact1_name in snapshot.branches
    assert snapshot.branches[b"HEAD"].target == branch_artifact2_name


def test_archive_snapshot_append_branch_override(swh_storage, requests_mock_datadir):
    # first loading for a first artifact
    artifact1 = GNU_ARTIFACTS[0]
    loader = ArchiveLoader(swh_storage, URL, [artifact1], snapshot_append=True)
    actual_load_status = loader.load()
    assert actual_load_status["status"] == "eventful"
    assert actual_load_status["snapshot_id"] is not None
    assert_last_visit_matches(swh_storage, URL, status="full", type="tar")

    # check expected snapshot
    snapshot = loader.last_snapshot()
    assert len(snapshot.branches) == 2
    branch_artifact1_name = f"releases/{artifact1['version']}".encode()
    assert branch_artifact1_name in snapshot.branches
    branch_target_first_visit = snapshot.branches[branch_artifact1_name].target

    # second loading for a second artifact with same version as the first one
    # but with different tarball content
    artifact2 = dict(GNU_ARTIFACTS[0])
    artifact2["url"] = GNU_ARTIFACTS[1]["url"]
    artifact2["time"] = GNU_ARTIFACTS[1]["time"]
    artifact2["length"] = GNU_ARTIFACTS[1]["length"]
    loader = ArchiveLoader(swh_storage, URL, [artifact2], snapshot_append=True)
    actual_load_status = loader.load()
    assert actual_load_status["status"] == "eventful"
    assert actual_load_status["snapshot_id"] is not None
    assert_last_visit_matches(swh_storage, URL, status="full", type="tar")

    # check expected snapshot, should contain the same branch as previously
    # but with different target
    snapshot = loader.last_snapshot()
    assert len(snapshot.branches) == 2
    assert branch_artifact1_name in snapshot.branches
    branch_target_second_visit = snapshot.branches[branch_artifact1_name].target

    assert branch_target_first_visit != branch_target_second_visit


@pytest.fixture
def not_gzipped_tarball_bytes(datadir):
    return Path(datadir, "not_gzipped_tarball.tar.gz").read_bytes()


def test_archive_not_gzipped_tarball(
    swh_storage, requests_mock, not_gzipped_tarball_bytes
):
    """Check that a tarball erroneously marked as gzip compressed can still
    be downloaded and processed.

    """
    filename = "not_gzipped_tarball.tar.gz"
    url = f"https://example.org/ftp/{filename}"
    requests_mock.get(
        url,
        [
            {"exc": ContentDecodingError,},
            {"body": BytesIO(not_gzipped_tarball_bytes),},
        ],
    )
    loader = ArchiveLoader(
        swh_storage,
        url,
        artifacts=[
            {
                "time": 944729610,
                "url": url,
                "length": 221837,
                "filename": filename,
                "version": "0.1.0",
            }
        ],
    )

    actual_load_status = loader.load()
    assert actual_load_status["status"] == "eventful"
    assert actual_load_status["snapshot_id"] is not None

    snapshot = loader.last_snapshot()
    assert len(snapshot.branches) == 2
    assert b"releases/0.1.0" in snapshot.branches
