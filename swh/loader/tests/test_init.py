# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import os
import subprocess

import attr
import pytest

from swh.loader.tests import (
    InconsistentAliasBranchError,
    InexistentObjectsError,
    assert_last_visit_matches,
    check_snapshot,
    encode_target,
    prepare_repository_from_archive,
)
from swh.model.from_disk import DentryPerms
from swh.model.hashutil import hash_to_bytes
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    ObjectType,
    OriginVisit,
    OriginVisitStatus,
    Person,
    Release,
    Revision,
    RevisionType,
    Snapshot,
    SnapshotBranch,
    TargetType,
    Timestamp,
    TimestampWithTimezone,
)

hash_hex = "43e45d56f88993aae6a0198013efa80716fd8920"


ORIGIN_VISIT = OriginVisit(
    origin="some-url",
    visit=1,
    date=datetime.datetime.now(tz=datetime.timezone.utc),
    type="archive",
)


ORIGIN_VISIT_STATUS = OriginVisitStatus(
    origin="some-url",
    visit=1,
    type="archive",
    date=datetime.datetime.now(tz=datetime.timezone.utc),
    status="full",
    snapshot=hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
    metadata=None,
)


CONTENT = Content(
    data=b"42\n",
    length=3,
    sha1=hash_to_bytes("34973274ccef6ab4dfaaf86599792fa9c3fe4689"),
    sha1_git=hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
    sha256=hash_to_bytes(
        "673650f936cb3b0a2f93ce09d81be10748b1b203c19e8176b4eefc1964a0cf3a"
    ),
    blake2s256=hash_to_bytes(
        "d5fe1939576527e42cfd76a9455a2432fe7f56669564577dd93c4280e76d661d"
    ),
    status="visible",
)


DIRECTORY = Directory(
    id=hash_to_bytes("34f335a750111ca0a8b64d8034faec9eedc396be"),
    entries=tuple(
        [
            DirectoryEntry(
                name=b"foo",
                type="file",
                target=CONTENT.sha1_git,
                perms=DentryPerms.content,
            )
        ]
    ),
)


REVISION = Revision(
    id=hash_to_bytes("066b1b62dbfa033362092af468bf6cfabec230e7"),
    message=b"hello",
    author=Person(
        name=b"Nicolas Dandrimont",
        email=b"nicolas@example.com",
        fullname=b"Nicolas Dandrimont <nicolas@example.com> ",
    ),
    date=TimestampWithTimezone(
        timestamp=Timestamp(seconds=1234567890, microseconds=0),
        offset=120,
        negative_utc=False,
    ),
    committer=Person(
        name=b"St\xc3fano Zacchiroli",
        email=b"stefano@example.com",
        fullname=b"St\xc3fano Zacchiroli <stefano@example.com>",
    ),
    committer_date=TimestampWithTimezone(
        timestamp=Timestamp(seconds=1123456789, microseconds=0),
        offset=0,
        negative_utc=True,
    ),
    parents=(),
    type=RevisionType.GIT,
    directory=DIRECTORY.id,
    metadata={
        "checksums": {"sha1": "tarball-sha1", "sha256": "tarball-sha256",},
        "signed-off-by": "some-dude",
    },
    extra_headers=(
        (b"gpgsig", b"test123"),
        (b"mergetag", b"foo\\bar"),
        (b"mergetag", b"\x22\xaf\x89\x80\x01\x00"),
    ),
    synthetic=True,
)


RELEASE = Release(
    id=hash_to_bytes("3e9050196aa288264f2a9d279d6abab8b158448b"),
    name=b"v0.0.2",
    author=Person(
        name=b"tony", email=b"tony@ardumont.fr", fullname=b"tony <tony@ardumont.fr>",
    ),
    date=TimestampWithTimezone(
        timestamp=Timestamp(seconds=1634336813, microseconds=0),
        offset=0,
        negative_utc=False,
    ),
    target=REVISION.id,
    target_type=ObjectType.REVISION,
    message=b"yet another synthetic release",
    synthetic=True,
)


SNAPSHOT = Snapshot(
    id=hash_to_bytes("2498dbf535f882bc7f9a18fb16c9ad27fda7bab7"),
    branches={
        b"release/0.1.0": SnapshotBranch(
            target=RELEASE.id, target_type=TargetType.RELEASE,
        ),
        b"HEAD": SnapshotBranch(target=REVISION.id, target_type=TargetType.REVISION,),
        b"alias": SnapshotBranch(target=b"HEAD", target_type=TargetType.ALIAS,),
        b"evaluation": SnapshotBranch(  # branch dedicated to not exist in storage
            target=hash_to_bytes("cc4e04c26672dd74e5fd0fecb78b435fb55368f7"),
            target_type=TargetType.REVISION,
        ),
    },
)


@pytest.fixture
def swh_storage_backend_config(swh_storage_postgresql):
    return {
        "cls": "local",
        "db": swh_storage_postgresql.dsn,
        "objstorage": {"cls": "memory"},
    }


@pytest.fixture
def mock_storage(mocker):
    mock_storage = mocker.patch("swh.loader.tests.origin_get_latest_visit_status")
    mock_storage.return_value = ORIGIN_VISIT_STATUS
    return mock_storage


def test_assert_last_visit_matches_raise(mock_storage, mocker):
    """Not finding origin visit_and_statu should raise

    """
    # overwrite so we raise because we do not find the right visit
    mock_storage.return_value = None

    with pytest.raises(AssertionError, match="Origin url has no visits"):
        assert_last_visit_matches(mock_storage, "url", status="full")

    assert mock_storage.called is True


def test_assert_last_visit_matches_wrong_status(mock_storage, mocker):
    """Wrong visit detected should raise AssertionError

    """
    expected_status = "partial"
    assert ORIGIN_VISIT_STATUS.status != expected_status
    with pytest.raises(AssertionError, match="Visit_status has status"):
        assert_last_visit_matches(mock_storage, "url", status=expected_status)

    assert mock_storage.called is True


def test_assert_last_visit_matches_wrong_type(mock_storage, mocker):
    """Wrong visit detected should raise AssertionError

    """
    expected_type = "git"
    assert ORIGIN_VISIT.type != expected_type
    with pytest.raises(AssertionError, match="Visit has type"):
        assert_last_visit_matches(
            mock_storage,
            "url",
            status=ORIGIN_VISIT_STATUS.status,
            type=expected_type,  # mismatched type will raise
        )

    assert mock_storage.called is True


def test_assert_last_visit_matches_wrong_snapshot(mock_storage, mocker):
    """Wrong visit detected should raise AssertionError

    """
    expected_snapshot_id = hash_to_bytes("e92cc0710eb6cf9efd5b920a8453e1e07157b6cd")
    assert ORIGIN_VISIT_STATUS.snapshot != expected_snapshot_id

    with pytest.raises(AssertionError, match="Visit_status points to snapshot"):
        assert_last_visit_matches(
            mock_storage,
            "url",
            status=ORIGIN_VISIT_STATUS.status,
            snapshot=expected_snapshot_id,  # mismatched snapshot will raise
        )

    assert mock_storage.called is True


def test_assert_last_visit_matches(mock_storage, mocker):
    """Correct visit detected should return the visit_status

    """
    visit_type = ORIGIN_VISIT.type
    visit_status = ORIGIN_VISIT_STATUS.status
    visit_snapshot = ORIGIN_VISIT_STATUS.snapshot

    actual_visit_status = assert_last_visit_matches(
        mock_storage,
        "url",
        type=visit_type,
        status=visit_status,
        snapshot=visit_snapshot,
    )

    assert actual_visit_status == ORIGIN_VISIT_STATUS
    assert mock_storage.called is True


def test_prepare_repository_from_archive_failure():
    # does not deal with inexistent archive so raise
    assert os.path.exists("unknown-archive") is False
    with pytest.raises(subprocess.CalledProcessError, match="exit status 2"):
        prepare_repository_from_archive("unknown-archive")


def test_prepare_repository_from_archive(datadir, tmp_path):
    archive_name = "0805nexter-1.1.0"
    archive_path = os.path.join(str(datadir), f"{archive_name}.tar.gz")
    assert os.path.exists(archive_path) is True

    tmp_path = str(tmp_path)  # deals with path string
    repo_url = prepare_repository_from_archive(
        archive_path, filename=archive_name, tmp_path=tmp_path
    )
    expected_uncompressed_archive_path = os.path.join(tmp_path, archive_name)
    assert repo_url == f"file://{expected_uncompressed_archive_path}"
    assert os.path.exists(expected_uncompressed_archive_path)


def test_prepare_repository_from_archive_no_filename(datadir, tmp_path):
    archive_name = "0805nexter-1.1.0"
    archive_path = os.path.join(str(datadir), f"{archive_name}.tar.gz")
    assert os.path.exists(archive_path) is True

    # deals with path as posix path (for tmp_path)
    repo_url = prepare_repository_from_archive(archive_path, tmp_path=tmp_path)

    tmp_path = str(tmp_path)
    expected_uncompressed_archive_path = os.path.join(tmp_path, archive_name)
    expected_repo_url = os.path.join(tmp_path, f"{archive_name}.tar.gz")
    assert repo_url == f"file://{expected_repo_url}"

    # passing along the filename does not influence the on-disk extraction
    # just the repo-url computation
    assert os.path.exists(expected_uncompressed_archive_path)


def test_encode_target():
    assert encode_target(None) is None

    for target_alias in ["something", b"something"]:
        target = {
            "target_type": "alias",
            "target": target_alias,
        }
        actual_alias_encode_target = encode_target(target)
        assert actual_alias_encode_target == {
            "target_type": "alias",
            "target": b"something",
        }

    for hash_ in [hash_hex, hash_to_bytes(hash_hex)]:
        target = {"target_type": "revision", "target": hash_}
        actual_encode_target = encode_target(target)
        assert actual_encode_target == {
            "target_type": "revision",
            "target": hash_to_bytes(hash_hex),
        }


def test_check_snapshot(swh_storage):
    """Everything should be fine when snapshot is found and the snapshot reference up to the
    revision exist in the storage.

    """
    # Create a consistent snapshot arborescence tree in storage
    found = False
    for entry in DIRECTORY.entries:
        if entry.target == CONTENT.sha1_git:
            found = True
            break
    assert found is True

    assert REVISION.directory == DIRECTORY.id
    assert RELEASE.target == REVISION.id

    for branch, target in SNAPSHOT.branches.items():
        if branch == b"alias":
            assert target.target in SNAPSHOT.branches
        elif branch == b"evaluation":
            # this one does not exist and we are safelisting its check below
            continue
        else:
            assert target.target in [REVISION.id, RELEASE.id]

    swh_storage.content_add([CONTENT])
    swh_storage.directory_add([DIRECTORY])
    swh_storage.revision_add([REVISION])
    swh_storage.release_add([RELEASE])
    s = swh_storage.snapshot_add([SNAPSHOT])
    assert s == {
        "snapshot:add": 1,
    }

    # all should be fine!
    check_snapshot(
        SNAPSHOT, swh_storage, allowed_empty=[(TargetType.REVISION, b"evaluation")]
    )


def test_check_snapshot_failures(swh_storage):
    """Failure scenarios:

    0. snapshot parameter is not a snapshot
    1. snapshot id is correct but branches mismatched
    2. snapshot id is not correct, it's not found in the storage
    3. snapshot reference an alias which does not exist
    4. snapshot is found in storage, targeted revision does not exist
    5. snapshot is found in storage, targeted revision exists but the directory the
       revision targets does not exist
    6. snapshot is found in storage, target revision exists, targeted directory by the
       revision exist. Content targeted by the directory does not exist.
    7. snapshot is found in storage, targeted release does not exist

    """
    snap_id_hex = "2498dbf535f882bc7f9a18fb16c9ad27fda7bab7"
    snapshot = Snapshot(
        id=hash_to_bytes(snap_id_hex),
        branches={
            b"master": SnapshotBranch(
                target=hash_to_bytes(hash_hex), target_type=TargetType.REVISION,
            ),
        },
    )

    s = swh_storage.snapshot_add([snapshot])
    assert s == {
        "snapshot:add": 1,
    }

    unexpected_snapshot = Snapshot(
        branches={
            b"tip": SnapshotBranch(  # wrong branch
                target=hash_to_bytes(hash_hex), target_type=TargetType.RELEASE
            )
        },
    )

    # 0. not a Snapshot object, raise!
    with pytest.raises(AssertionError, match="variable 'snapshot' must be a snapshot"):
        check_snapshot(ORIGIN_VISIT, swh_storage)

    # 1. snapshot id is correct but branches mismatched
    with pytest.raises(AssertionError):  # sadly debian build raises only assertion
        check_snapshot(attr.evolve(unexpected_snapshot, id=snapshot.id), swh_storage)

    # 2. snapshot id is not correct, it's not found in the storage
    wrong_snap_id = hash_to_bytes("999666f535f882bc7f9a18fb16c9ad27fda7bab7")
    with pytest.raises(AssertionError, match="is not found"):
        check_snapshot(attr.evolve(unexpected_snapshot, id=wrong_snap_id), swh_storage)

    # 3. snapshot references an inexistent alias
    snapshot0 = Snapshot(
        id=hash_to_bytes("123666f535f882bc7f9a18fb16c9ad27fda7bab7"),
        branches={
            b"alias": SnapshotBranch(target=b"HEAD", target_type=TargetType.ALIAS,),
        },
    )
    swh_storage.snapshot_add([snapshot0])

    with pytest.raises(InconsistentAliasBranchError, match="Alias branch HEAD"):
        check_snapshot(snapshot0, swh_storage)

    # 4. snapshot is found in storage, targeted revision does not exist

    rev_not_found = list(swh_storage.revision_missing([REVISION.id]))
    assert len(rev_not_found) == 1

    snapshot1 = Snapshot(
        id=hash_to_bytes("456666f535f882bc7f9a18fb16c9ad27fda7bab7"),
        branches={
            b"alias": SnapshotBranch(target=b"HEAD", target_type=TargetType.ALIAS,),
            b"HEAD": SnapshotBranch(
                target=REVISION.id, target_type=TargetType.REVISION,
            ),
        },
    )

    swh_storage.snapshot_add([snapshot1])

    with pytest.raises(InexistentObjectsError, match="Branch/Revision"):
        check_snapshot(snapshot1, swh_storage)

    # 5. snapshot is found in storage, targeted revision exists but the directory the
    # revision targets does not exist

    swh_storage.revision_add([REVISION])

    dir_not_found = list(swh_storage.directory_missing([REVISION.directory]))
    assert len(dir_not_found) == 1

    snapshot2 = Snapshot(
        id=hash_to_bytes("987123f535f882bc7f9a18fb16c9ad27fda7bab7"),
        branches={
            b"alias": SnapshotBranch(target=b"HEAD", target_type=TargetType.ALIAS,),
            b"HEAD": SnapshotBranch(
                target=REVISION.id, target_type=TargetType.REVISION,
            ),
        },
    )

    swh_storage.snapshot_add([snapshot2])
    with pytest.raises(InexistentObjectsError, match="Missing directories"):
        check_snapshot(snapshot2, swh_storage)

    assert DIRECTORY.id == REVISION.directory
    swh_storage.directory_add([DIRECTORY])

    # 6. snapshot is found in storage, target revision exists, targeted directory by the
    # revision exist. Content targeted by the directory does not exist.

    assert DIRECTORY.entries[0].target == CONTENT.sha1_git
    not_found = list(swh_storage.content_missing_per_sha1_git([CONTENT.sha1_git]))
    assert len(not_found) == 1

    swh_storage.directory_add([DIRECTORY])

    snapshot3 = Snapshot(
        id=hash_to_bytes("091456f535f882bc7f9a18fb16c9ad27fda7bab7"),
        branches={
            b"alias": SnapshotBranch(target=b"HEAD", target_type=TargetType.ALIAS,),
            b"HEAD": SnapshotBranch(
                target=REVISION.id, target_type=TargetType.REVISION,
            ),
        },
    )

    swh_storage.snapshot_add([snapshot3])
    with pytest.raises(InexistentObjectsError, match="Missing content(s)"):
        check_snapshot(snapshot3, swh_storage)

    # 7. snapshot is found in storage, targeted release does not exist

    # release targets the revisions which exists
    assert RELEASE.target == REVISION.id

    snapshot4 = Snapshot(
        id=hash_to_bytes("789666f535f882bc7f9a18fb16c9ad27fda7bab7"),
        branches={
            b"alias": SnapshotBranch(target=b"HEAD", target_type=TargetType.ALIAS,),
            b"HEAD": SnapshotBranch(
                target=REVISION.id, target_type=TargetType.REVISION,
            ),
            b"release/0.1.0": SnapshotBranch(
                target=RELEASE.id, target_type=TargetType.RELEASE,
            ),
        },
    )

    swh_storage.snapshot_add([snapshot4])

    with pytest.raises(InexistentObjectsError, match="Branch/Release"):
        check_snapshot(snapshot4, swh_storage)
