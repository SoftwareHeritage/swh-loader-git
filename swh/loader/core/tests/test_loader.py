# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.model.hashutil import hash_to_bytes
from swh.storage.in_memory import Storage

from . import BaseLoaderTest


class DummyBaseLoaderTest(BaseLoaderTest):
    def setUp(self):
        # do not call voluntarily super().setUp()
        self.storage = Storage()
        contents = [
            {
                'id': '34973274ccef6ab4dfaaf86599792fa9c3fe4689',
                'sha1': '34973274ccef6ab4dfaaf86599792fa9c3fe4689',
                'sha1_git': b'bar1',
                'sha256': b'baz1',
                'blake2s256': b'qux1',
                'status': 'visible',
                'data': b'data1',
            },
            {
                'id': '61c2b3a30496d329e21af70dd2d7e097046d07b7',
                'sha1': '61c2b3a30496d329e21af70dd2d7e097046d07b7',
                'sha1_git': b'bar2',
                'sha256': b'baz2',
                'blake2s256': b'qux2',
                'status': 'visible',
                'data': b'data2',
            },
        ]
        self.expected_contents = [content['id'] for content in contents]
        self.in_contents = contents.copy()
        for content in self.in_contents:
            content['sha1'] = hash_to_bytes(content['sha1'])
        self.in_directories = [
            {'id': hash_to_bytes(id_)}
            for id_ in [
                '44e45d56f88993aae6a0198013efa80716fd8921',
                '54e45d56f88993aae6a0198013efa80716fd8920',
                '43e45d56f88993aae6a0198013efa80716fd8920',
            ]
        ]
        self.in_revisions = [
            {
                'id': b'rev1',
                'date': None,
            },
        ]
        self.in_releases = [
            {
                'id': b'rel1',
                'date': None,
            },
        ]
        self.in_origins = [
            {
                'type': 'git',
                'url': 'http://example.com/',
            },
        ]
        self.in_snapshot = {
            'id': b'snap1',
            'branches': {},
        }

    def tearDown(self):
        # do not call voluntarily super().tearDown()
        pass


class LoadTest1(DummyBaseLoaderTest):
    def setUp(self):
        super().setUp()

    def test_stateful_loader(self):
        """Stateful loader accumulates in place the sent data

        Note: Those behaviors should be somehow merged but that's
        another story.

        """
        self.storage.directory_add(self.in_directories)
        self.storage.revision_add(self.in_revisions)
        self.storage.release_add(self.in_releases)

        self.assertCountContents(0)
        self.assertCountDirectories(len(self.in_directories))
        self.assertCountRevisions(len(self.in_revisions))
        self.assertCountSnapshots(0)

    def test_stateless_loader(self):
        """Stateless loader accumulates in place the sent data as well

        Note: Those behaviors should be somehow merged but that's
        another story.

        """
        (origin,) = self.storage.origin_add(self.in_origins)
        visit = self.storage.origin_visit_add(
            origin['id'], datetime.datetime.utcnow())
        self.storage.content_add(self.in_contents)
        self.storage.snapshot_add(origin, visit['visit'], self.in_snapshot)

        self.assertCountContents(len(self.in_contents))
        self.assertCountDirectories(0)
        self.assertCountRevisions(0)
        self.assertCountReleases(0)
        self.assertCountSnapshots(1)


class LoadTestContent(DummyBaseLoaderTest):
    def test_load_contents(self):
        """Loading contents should be ok

        """
        self.storage.content_add(self.in_contents)
        self.assertCountContents(len(self.expected_contents))
        self.assertContentsOk(self.expected_contents)

    def test_failing(self):
        """Comparing wrong snapshot should fail.

        """
        self.storage.content_add(self.in_contents)
        with self.assertRaises(AssertionError):
            self.assertContentsOk([])


class LoadTestDirectory(DummyBaseLoaderTest):
    def test_send_batch_directories(self):
        """Loading directories should be ok

        """
        self.storage.directory_add(self.in_directories)
        self.assertCountDirectories(len(self.in_directories))
        self.assertDirectoriesOk(self.in_directories)

    def test_failing(self):
        """Comparing wrong snapshot should fail.

        """
        self.storage.directory_add(self.in_directories)
        with self.assertRaises(AssertionError):
            self.assertDirectoriesOk([])
