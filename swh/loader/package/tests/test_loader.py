# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import attr

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


def test_loader_origin_visit_failure(swh_config):
    """Failure to add origin or origin visit should failed immediately

    """
    loader = PackageLoader("some-url")
    loader.storage = FakeStorage()

    actual_load_status = loader.load()
    assert actual_load_status == {"status": "failed"}

    loader.storage = FakeStorage2()

    actual_load_status2 = loader.load()
    assert actual_load_status2 == {"status": "failed"}


def test_artifact_identity():
    """Compute primary key should return the right identity

    """

    @attr.s
    class TestPackageInfo(BasePackageInfo):
        a = attr.ib()
        b = attr.ib()
        length = attr.ib()
        filename = attr.ib()
        version = attr.ib()

        ID_KEYS = ["a", "b"]

    p_info = TestPackageInfo(
        url="http://example.org/",
        a=1,
        b=2,
        length=221837,
        filename="8sync-0.1.0.tar.gz",
        version="0.1.0",
    )

    actual_id = p_info.artifact_identity()
    assert actual_id == [1, 2]
