# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.package.loader import PackageLoader


class FakeStorage:
    def origin_add_one(self, origin):
        raise ValueError('We refuse to add an origin')


class FakeStorage2:
    def origin_add_one(self, origin):
        return origin

    def origin_visit_add(self, origin, date, type):
        raise ValueError('We refuse to add an origin visit')


def test_loader_origin_visit_failure(swh_config):
    """Failure to add origin or origin visit should failed immediately

    """
    loader = PackageLoader('some-url')
    loader.storage = FakeStorage()

    actual_load_status = loader.load()
    assert actual_load_status == {'status': 'failed'}

    loader.storage = FakeStorage2()

    actual_load_status2 = loader.load()
    assert actual_load_status2 == {'status': 'failed'}
