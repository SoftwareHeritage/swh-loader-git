# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import os
import requests_mock

from typing import List

from swh.loader.package.loader import GNULoader
from swh.loader.core.tests import BaseLoaderStorageTest
from swh.loader.package.tests.common import (
    package, package_url,
    tarball, init_test_data
)

_LOADER_TESTS_CONFIG = {
    'content_packet_size': 10000,
    'content_packet_size_bytes': 104857600,
    'content_size_limit': 104857600,
    'debug': False,
    'directory_packet_size': 25000,
    'occurrence_packet_size': 100000,
    'release_packet_size': 100000,
    'revision_packet_size': 100000,
    'send_contents': True,
    'send_directories': True,
    'send_releases': True,
    'send_revisions': True,
    'send_snapshot': True,
    'storage': {'args': {}, 'cls': 'memory'},
    'temp_directory': '/tmp/swh.loader.gnu/'
}


class GNULoaderTest(GNULoader):
    def parse_config_file(self, *args, **kwargs):
        return _LOADER_TESTS_CONFIG


@requests_mock.Mocker()
class TestGNULoader(unittest.TestCase, BaseLoaderStorageTest):

    _expected_new_contents_first_visit = [
        'e9258d81faf5881a2f96a77ba609396f82cb97ad',
        '1170cf105b04b7e2822a0e09d2acf71da7b9a130',
        'fbd27c3f41f2668624ffc80b7ba5db9b92ff27ac',
        '0057bec9b5422aff9256af240b177ac0e3ac2608',
        '2b8d0d0b43a1078fc708930c8ddc2956a86c566e',
        '27de3b3bc6545d2a797aeeb4657c0e215a0c2e55',
        '2e6db43f5cd764e677f416ff0d0c78c7a82ef19b',
        'ae9be03bd2a06ed8f4f118d3fe76330bb1d77f62',
        'edeb33282b2bffa0e608e9d2fd960fd08093c0ea',
        'd64e64d4c73679323f8d4cde2643331ba6c20af9',
        '7a756602914be889c0a2d3952c710144b3e64cb0',
        '84fb589b554fcb7f32b806951dcf19518d67b08f',
        '8624bcdae55baeef00cd11d5dfcfa60f68710a02',
        'e08441aeab02704cfbd435d6445f7c072f8f524e',
        'f67935bc3a83a67259cda4b2d43373bd56703844',
        '809788434b433eb2e3cfabd5d591c9a659d5e3d8',
        '7d7c6c8c5ebaeff879f61f37083a3854184f6c41',
        'b99fec102eb24bffd53ab61fc30d59e810f116a2',
        '7d149b28eaa228b3871c91f0d5a95a2fa7cb0c68',
        'f0c97052e567948adf03e641301e9983c478ccff',
        '7fb724242e2b62b85ca64190c31dcae5303e19b3',
        '4f9709e64a9134fe8aefb36fd827b84d8b617ab5',
        '7350628ccf194c2c3afba4ac588c33e3f3ac778d',
        '0bb892d9391aa706dc2c3b1906567df43cbe06a2',
        '49d4c0ce1a16601f1e265d446b6c5ea6b512f27c',
        '6b5cc594ac466351450f7f64a0b79fdaf4435ad3',
        '3046e5d1f70297e2a507b98224b6222c9688d610',
        '1572607d456d7f633bc6065a2b3048496d679a31',
    ]

    _expected_new_directories_first_visit = [
        'daabc65ec75d487b1335ffc101c0ac11c803f8fc',
        '263be23b4a8101d3ad0d9831319a3e0f2b065f36',
        '7f6e63ba6eb3e2236f65892cd822041f1a01dd5c',
        '4db0a3ecbc976083e2dac01a62f93729698429a3',
        'dfef1c80e1098dd5deda664bb44a9ab1f738af13',
        'eca971d346ea54d95a6e19d5051f900237fafdaa',
        '3aebc29ed1fccc4a6f2f2010fb8e57882406b528',
    ]

    _expected_new_revisions_first_visit = {
        '44183488c0774ce3c957fa19ba695cf18a4a42b3':
        '3aebc29ed1fccc4a6f2f2010fb8e57882406b528'
        }

    _expected_branches_first_visit = {
        'HEAD': {
            'target': 'release/8sync-0.1.0',
            'target_type': 'alias'
        },
        'release/8sync-0.1.0': {
            'target': '44183488c0774ce3c957fa19ba695cf18a4a42b3',
            'target_type': 'revision'
        },
    }
    _expected_new_snapshot_first_visit = '2ae491bbaeef7351641997d1b9193aa2a67d26bc' # noqa

    _expected_new_contents_invalid_origin = []  # type: List[str]
    _expected_new_directories_invalid_origin = []  # type: List[str]

    @classmethod
    def setUpClass(cls):
        cls.reset_loader()

    @classmethod
    def reset_loader(cls):
        cls.loader = GNULoaderTest()
        cls.storage = cls.loader.storage

    def reset_loader_counters(self):
        counters_reset = dict.fromkeys(self.loader.counters.keys(), 0)
        self.loader.counters.update(counters_reset)

    def test_gnu_loader_first_visit_success(self, mock_tarball_request):
        """In this scenario no visit as taken place prior to this visit.

        """
        self.reset_loader()
        init_test_data(mock_tarball_request)
        self.loader.load(package, package_url, tarballs=tarball)

        self.assertCountContents(len(self._expected_new_contents_first_visit))
        self.assertContentsContain(self._expected_new_contents_first_visit)
        self.assertEqual(self.loader.counters['contents'],
                         len(self._expected_new_contents_first_visit))

        self.assertCountDirectories(len(self._expected_new_directories_first_visit)) # noqa
        self.assertDirectoriesContain(self._expected_new_directories_first_visit)   # noqa
        self.assertEqual(self.loader.counters['directories'],
                         len(self._expected_new_directories_first_visit))

        self.assertCountRevisions(1, '1 artifact releases so 1 revisions should be created') # noqa
        self.assertRevisionsContain(self._expected_new_revisions_first_visit)
        self.assertEqual(self.loader.counters['revisions'],
                         len(self._expected_new_revisions_first_visit))

        self.assertCountReleases(0, 'No release is created by the loader')
        self.assertEqual(self.loader.counters['releases'], 0)

        self.assertCountSnapshots(1, 'Only 1 snapshot targeting all revisions')
        self.assertSnapshotEqual(self._expected_new_snapshot_first_visit,
                                 self._expected_branches_first_visit)

        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        self.assertFalse(os.path.exists(self.loader.temp_directory))

    def test_gnu_loader_origin_invalid(self, mock_tarball_request):
        """In this scenario, tarball link is not valid and will give 404 error

        """
        self.reset_loader()
        mock_tarball_request.get(
            'https://ftp.gnu.org/gnu/8sync/8sync-0.1.0.tar.gz',
            text='Not Found', status_code=404)
        self.loader.load(package, package_url, tarballs=tarball)

        self.assertContentsContain(self._expected_new_contents_invalid_origin)
        self.assertCountContents(len(self._expected_new_contents_invalid_origin)) # noqa
        self.assertEqual(self.loader.counters['contents'],
                         len(self._expected_new_contents_invalid_origin))

        self.assertDirectoriesContain(self._expected_new_directories_invalid_origin)   # noqa
        self.assertCountDirectories(len(self._expected_new_directories_invalid_origin)) # noqa
        self.assertEqual(self.loader.counters['directories'],
                         len(self._expected_new_directories_invalid_origin))

        self.assertCountRevisions(0, '0 releases so 0 revisions should be created') # noqa

        self.assertEqual(self.loader.counters['releases'], 0)
        self.assertCountReleases(0, 'No release is created by the loader')

        self.assertCountSnapshots(1, 'Only 1 snapshot targeting all revisions')

        self.assertEqual(self.loader.load_status(), {'status': 'uneventful'})
        self.assertEqual(self.loader.visit_status(), 'partial')

        self.assertFalse(os.path.exists(self.loader.temp_directory))

    def test_gnu_loader_second_visit(self, mock_tarball_request):
        """This scenario makes use of the incremental nature of the loader.

        In this test there is no change from the first visit. So same result
        as first visit.
        """
        self.reset_loader()
        init_test_data(mock_tarball_request)
        self.loader.load(package, package_url, tarballs=tarball)

        self.assertCountContents(len(self._expected_new_contents_first_visit))
        self.assertContentsContain(self._expected_new_contents_first_visit)
        self.assertEqual(self.loader.counters['contents'],
                         len(self._expected_new_contents_first_visit))

        self.assertCountDirectories(len(self._expected_new_directories_first_visit)) # noqa
        self.assertDirectoriesContain(self._expected_new_directories_first_visit)   # noqa
        self.assertEqual(self.loader.counters['directories'],
                         len(self._expected_new_directories_first_visit))

        self.assertCountRevisions(1, '1 artifact releases so 1 revisions should be created') # noqa
        self.assertRevisionsContain(self._expected_new_revisions_first_visit)
        self.assertEqual(self.loader.counters['revisions'],
                         len(self._expected_new_revisions_first_visit))

        self.assertCountReleases(0, 'No release is created by the loader')
        self.assertEqual(self.loader.counters['releases'], 0)

        self.assertCountSnapshots(1, 'Only 1 snapshot targeting all revisions')
        self.assertSnapshotEqual(self._expected_new_snapshot_first_visit,
                                 self._expected_branches_first_visit)

        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        self.assertFalse(os.path.exists(self.loader.temp_directory))
