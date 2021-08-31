# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import hashlib
import string
from unittest.mock import Mock, call, patch

import attr
import pytest

from swh.loader.package.loader import BasePackageInfo, PackageLoader
from swh.model.identifiers import CoreSWHID, ObjectType
from swh.model.model import (
    ExtID,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.storage import get_storage
from swh.storage.algos.snapshot import snapshot_get_latest


class FakeStorage:
    def origin_add(self, origins):
        raise ValueError("We refuse to add an origin")

    def origin_visit_get_latest(self, origin):
        return None


class FakeStorage2(FakeStorage):
    def origin_add(self, origins):
        pass

    def origin_visit_add(self, visits):
        raise ValueError("We refuse to add an origin visit")


class StubPackageInfo(BasePackageInfo):
    pass


class StubPackageLoader(PackageLoader[StubPackageInfo]):
    def get_versions(self):
        return ["v1.0", "v2.0", "v3.0", "v4.0"]

    def get_package_info(self, version):
        p_info = StubPackageInfo("http://example.org", f"example-{version}.tar")
        extid_type = "extid-type1" if version in ("v1.0", "v2.0") else "extid-type2"
        # Versions 1.0 and 2.0 have an extid of a given type, v3.0 has an extid
        # of a different type
        patch.object(
            p_info,
            "extid",
            return_value=(extid_type, f"extid-of-{version}".encode()),
            autospec=True,
        ).start()
        yield (f"branch-{version}", p_info)

    def _load_revision(self, p_info, origin):
        return None


def test_loader_origin_visit_failure(swh_storage):
    """Failure to add origin or origin visit should failed immediately

    """
    loader = PackageLoader(swh_storage, "some-url")
    loader.storage = FakeStorage()

    actual_load_status = loader.load()
    assert actual_load_status == {"status": "failed"}

    loader.storage = FakeStorage2()

    actual_load_status2 = loader.load()
    assert actual_load_status2 == {"status": "failed"}


def test_resolve_revision_from_extids() -> None:
    loader = PackageLoader(None, None)  # type: ignore

    p_info = Mock(wraps=BasePackageInfo(None, None))  # type: ignore

    # The PackageInfo does not support extids
    p_info.extid.return_value = None
    known_extids = {
        ("extid-type", b"extid-of-aaaa"): [
            CoreSWHID(object_type=ObjectType.REVISION, object_id=b"a" * 20),
        ]
    }
    revision_whitelist = {b"unused"}
    assert (
        loader.resolve_revision_from_extids(known_extids, p_info, revision_whitelist)
        is None
    )

    # Some known extid, and the PackageInfo is not one of them (ie. cache miss)
    p_info.extid.return_value = ("extid-type", b"extid-of-cccc")
    assert (
        loader.resolve_revision_from_extids(known_extids, p_info, revision_whitelist)
        is None
    )

    # Some known extid, and the PackageInfo is one of them (ie. cache hit),
    # but the target revision was not in the previous snapshot
    p_info.extid.return_value = ("extid-type", b"extid-of-aaaa")
    assert (
        loader.resolve_revision_from_extids(known_extids, p_info, revision_whitelist)
        is None
    )

    # Some known extid, and the PackageInfo is one of them (ie. cache hit),
    # and the target revision was in the previous snapshot
    revision_whitelist = {b"a" * 20}
    assert (
        loader.resolve_revision_from_extids(known_extids, p_info, revision_whitelist)
        == b"a" * 20
    )

    # Same as before, but there is more than one extid, and only one is an allowed
    # revision
    revision_whitelist = {b"a" * 20}
    known_extids = {
        ("extid-type", b"extid-of-aaaa"): [
            CoreSWHID(object_type=ObjectType.REVISION, object_id=b"b" * 20),
            CoreSWHID(object_type=ObjectType.REVISION, object_id=b"a" * 20),
        ]
    }
    assert (
        loader.resolve_revision_from_extids(known_extids, p_info, revision_whitelist)
        == b"a" * 20
    )


def test_load_get_known_extids() -> None:
    """Checks PackageLoader.load() fetches known extids efficiently"""
    storage = Mock(wraps=get_storage("memory"))

    loader = StubPackageLoader(storage, "http://example.org")

    loader.load()

    # Calls should be grouped by extid type
    storage.extid_get_from_extid.assert_has_calls(
        [
            call("extid-type1", [b"extid-of-v1.0", b"extid-of-v2.0"]),
            call("extid-type2", [b"extid-of-v3.0", b"extid-of-v4.0"]),
        ],
        any_order=True,
    )


def test_load_extids() -> None:
    """Checks PackageLoader.load() skips iff it should, and writes (only)
    the new ExtIDs"""
    storage = get_storage("memory")

    origin = "http://example.org"
    rev1_swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=b"a" * 20)
    rev2_swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=b"b" * 20)
    rev3_swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=b"c" * 20)
    rev4_swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=b"d" * 20)
    dir_swhid = CoreSWHID(object_type=ObjectType.DIRECTORY, object_id=b"e" * 20)

    # Results of a previous load
    storage.extid_add(
        [
            ExtID("extid-type1", b"extid-of-v1.0", rev1_swhid),
            ExtID("extid-type2", b"extid-of-v2.0", rev2_swhid),
        ]
    )
    last_snapshot = Snapshot(
        branches={
            b"v1.0": SnapshotBranch(
                target_type=TargetType.REVISION, target=rev1_swhid.object_id
            ),
            b"v2.0": SnapshotBranch(
                target_type=TargetType.REVISION, target=rev2_swhid.object_id
            ),
            b"v3.0": SnapshotBranch(
                target_type=TargetType.REVISION, target=rev3_swhid.object_id
            ),
        }
    )
    storage.snapshot_add([last_snapshot])
    date = datetime.datetime.now(tz=datetime.timezone.utc)
    storage.origin_add([Origin(url=origin)])
    storage.origin_visit_add(
        [OriginVisit(origin="http://example.org", visit=1, date=date, type="tar")]
    )
    storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=origin,
                visit=1,
                status="full",
                date=date,
                snapshot=last_snapshot.id,
            )
        ]
    )

    loader = StubPackageLoader(storage, "http://example.org")
    patch.object(
        loader,
        "_load_revision",
        return_value=(rev4_swhid.object_id, dir_swhid.object_id),
        autospec=True,
    ).start()

    loader.load()

    assert loader._load_revision.mock_calls == [  # type: ignore
        # v1.0: not loaded because there is already its (extid_type, extid, rev)
        #       in the storage.
        # v2.0: loaded, because there is already a similar extid, but different type
        call(StubPackageInfo(origin, "example-v2.0.tar"), Origin(url=origin)),
        # v3.0: loaded despite having an (extid_type, extid) in storage, because
        #       the target of the extid is not in the previous snapshot
        call(StubPackageInfo(origin, "example-v3.0.tar"), Origin(url=origin)),
        # v4.0: loaded, because there isn't its extid
        call(StubPackageInfo(origin, "example-v4.0.tar"), Origin(url=origin)),
    ]

    # then check the snapshot has all the branches.
    # versions 2.0 to 4.0 all point to rev4_swhid (instead of the value of the last
    # snapshot), because they had to be loaded (mismatched extid), and the mocked
    # _load_revision always returns rev4_swhid.
    snapshot = Snapshot(
        branches={
            b"branch-v1.0": SnapshotBranch(
                target_type=TargetType.REVISION, target=rev1_swhid.object_id
            ),
            b"branch-v2.0": SnapshotBranch(
                target_type=TargetType.REVISION, target=rev4_swhid.object_id
            ),
            b"branch-v3.0": SnapshotBranch(
                target_type=TargetType.REVISION, target=rev4_swhid.object_id
            ),
            b"branch-v4.0": SnapshotBranch(
                target_type=TargetType.REVISION, target=rev4_swhid.object_id
            ),
        }
    )
    assert snapshot_get_latest(storage, origin) == snapshot

    extids = storage.extid_get_from_target(
        ObjectType.REVISION,
        [
            rev1_swhid.object_id,
            rev2_swhid.object_id,
            rev3_swhid.object_id,
            rev4_swhid.object_id,
        ],
    )

    assert set(extids) == {
        # What we inserted at the beginning of the test:
        ExtID("extid-type1", b"extid-of-v1.0", rev1_swhid),
        ExtID("extid-type2", b"extid-of-v2.0", rev2_swhid),
        # Added by the loader:
        ExtID("extid-type1", b"extid-of-v2.0", rev4_swhid),
        ExtID("extid-type2", b"extid-of-v3.0", rev4_swhid),
        ExtID("extid-type2", b"extid-of-v4.0", rev4_swhid),
    }


def test_manifest_extid():
    """Compute primary key should return the right identity

    """

    @attr.s
    class TestPackageInfo(BasePackageInfo):
        a = attr.ib()
        b = attr.ib()
        length = attr.ib()
        filename = attr.ib()
        version = attr.ib()

        MANIFEST_FORMAT = string.Template("$a $b")

    p_info = TestPackageInfo(
        url="http://example.org/",
        a=1,
        b=2,
        length=221837,
        filename="8sync-0.1.0.tar.gz",
        version="0.1.0",
    )

    actual_id = p_info.extid()
    assert actual_id == ("package-manifest-sha256", hashlib.sha256(b"1 2").digest())


def test_no_env_swh_config_filename_raise(monkeypatch):
    """No SWH_CONFIG_FILENAME environment variable makes package loader init raise

    """

    class DummyPackageLoader(PackageLoader):
        """A dummy package loader for test purpose"""

        pass

    monkeypatch.delenv("SWH_CONFIG_FILENAME", raising=False)

    with pytest.raises(
        AssertionError, match="SWH_CONFIG_FILENAME environment variable is undefined"
    ):
        DummyPackageLoader.from_configfile(url="some-url")
