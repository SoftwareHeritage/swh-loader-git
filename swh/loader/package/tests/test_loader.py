# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib
import string
from unittest.mock import MagicMock

import attr
import pytest

from swh.loader.package.loader import BasePackageInfo, PackageLoader


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


def test_resolve_revision_from_artifacts():
    loader = PackageLoader(None, None)
    loader.known_artifact_to_extid = MagicMock(
        wraps=lambda known_artifact: known_artifact["key"].encode()
    )

    known_artifacts = {
        b"a" * 40: {"key": "extid-of-aaaa"},
        b"b" * 40: {"key": "extid-of-bbbb"},
    }

    p_info = MagicMock()

    # No known artifact -> it would be useless to compute the extid
    assert loader.resolve_revision_from_artifacts({}, p_info) is None
    p_info.extid.assert_not_called()
    loader.known_artifact_to_extid.assert_not_called()

    p_info.extid.reset_mock()

    # Some artifacts, but the PackageInfo does not support extids
    p_info.extid.return_value = None
    assert loader.resolve_revision_from_artifacts(known_artifacts, p_info) is None
    p_info.extid.assert_called_once()
    loader.known_artifact_to_extid.assert_not_called()

    p_info.extid.reset_mock()

    # Some artifacts, and the PackageInfo is not one of them (ie. cache miss)
    p_info.extid.return_value = b"extid-of-cccc"
    assert loader.resolve_revision_from_artifacts(known_artifacts, p_info) is None
    p_info.extid.assert_called_once()
    loader.known_artifact_to_extid.assert_any_call({"key": "extid-of-aaaa"})
    loader.known_artifact_to_extid.assert_any_call({"key": "extid-of-bbbb"})

    p_info.extid.reset_mock()
    loader.known_artifact_to_extid.reset_mock()

    # Some artifacts, and the PackageInfo is one of them (ie. cache hit)
    p_info.extid.return_value = b"extid-of-aaaa"
    assert loader.resolve_revision_from_artifacts(known_artifacts, p_info) == b"a" * 40
    p_info.extid.assert_called_once()
    loader.known_artifact_to_extid.assert_called_once_with({"key": "extid-of-aaaa"})


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
