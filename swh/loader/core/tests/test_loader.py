# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from nose.tools import istest

from . import BaseLoaderTest, LoaderNoStorage

from swh.model.hashutil import hash_to_bytes


class DummyBaseLoaderTest(BaseLoaderTest):
    def setUp(self):
        # do not call voluntarily super().setUp()
        self.in_contents = [1, 2, 3]
        self.in_directories = [4, 5, 6]
        self.in_revisions = [7, 8, 9]
        self.in_releases = [10, 11, 12]
        self.in_snapshot = 13

    def tearDown(self):
        # do not call voluntarily super().tearDown()
        pass


class LoadTest1(DummyBaseLoaderTest):
    def setUp(self):
        super().setUp()
        self.loader = LoaderNoStorage()

    @istest
    def stateful_loader(self):
        """Stateful loader accumulates in place the sent data

        Note: Those behaviors should be somehow merged but that's
        another story.

        """
        self.loader.maybe_load_directories(self.in_directories)
        self.loader.maybe_load_revisions(self.in_revisions)
        self.loader.maybe_load_releases(self.in_releases)

        self.assertEquals(len(self.state('content')), 0)
        self.assertEquals(
            len(self.state('directory')), len(self.in_directories))
        self.assertEquals(
            len(self.state('revision')), len(self.in_revisions))
        self.assertEquals(
            len(self.state('release')), len(self.in_releases))
        self.assertEquals(len(self.state('snapshot')), 0)

    @istest
    def stateless_loader(self):
        """Stateless loader accumulates in place the sent data as well

        Note: Those behaviors should be somehow merged but that's
        another story.

        """
        self.loader.send_batch_contents(self.in_contents)
        self.loader.send_snapshot(self.in_snapshot)

        self.assertEquals(len(self.state('content')), len(self.in_contents))
        self.assertEquals(len(self.state('directory')), 0)
        self.assertEquals(len(self.state('revision')), 0)
        self.assertEquals(len(self.state('release')), 0)
        self.assertEquals(len(self.state('snapshot')), 1)


class LoadTestContent(DummyBaseLoaderTest):
    def setUp(self):
        super().setUp()
        self.loader = LoaderNoStorage()

        self.content_id0 = '34973274ccef6ab4dfaaf86599792fa9c3fe4689'
        self.content_id1 = '61c2b3a30496d329e21af70dd2d7e097046d07b7'
        # trimmed data to the bare necessities
        self.in_contents = [{
            'sha1': hash_to_bytes(self.content_id0),
        }, {
            'sha1': hash_to_bytes(self.content_id1),
        }]
        self.expected_contents = [self.content_id0, self.content_id1]

    @istest
    def maybe_load_contents(self):
        """Loading contents should be ok

        """
        self.loader.maybe_load_contents(self.in_contents)
        self.assertCountContents(len(self.expected_contents))
        self.assertContentsOk(self.expected_contents)

    @istest
    def send_batch_contents(self):
        """Sending contents should be ok 2

        """
        self.loader.send_batch_contents(self.in_contents)
        self.assertCountContents(len(self.expected_contents))
        self.assertContentsOk(self.expected_contents)

    @istest
    def failing(self):
        """Comparing wrong snapshot should fail.

        """
        self.loader.send_batch_contents(self.in_contents)
        with self.assertRaises(AssertionError):
            self.assertContentsOk([])


class LoadTestDirectory(DummyBaseLoaderTest):
    def setUp(self):
        super().setUp()
        self.loader = LoaderNoStorage()

        self.directory_id0 = '44e45d56f88993aae6a0198013efa80716fd8921'
        self.directory_id1 = '54e45d56f88993aae6a0198013efa80716fd8920'
        self.directory_id2 = '43e45d56f88993aae6a0198013efa80716fd8920'
        # trimmed data to the bare necessities
        self.in_directories = [{
            'id': hash_to_bytes(self.directory_id0),
        }, {
            'id': hash_to_bytes(self.directory_id1),
        }, {
            'id': hash_to_bytes(self.directory_id2),
        }]
        self.expected_directories = [
            self.directory_id0, self.directory_id1, self.directory_id2]

    @istest
    def maybe_load_directories(self):
        """Loading directories should be ok

        """
        self.loader.maybe_load_directories(self.in_directories)
        self.assertCountDirectories(len(self.expected_directories))
        self.assertDirectoriesOk(self.expected_directories)

    @istest
    def send_batch_directories(self):
        """Sending directories should be ok 2

        """
        self.loader.send_batch_directories(self.in_directories)
        self.assertCountDirectories(len(self.expected_directories))
        self.assertDirectoriesOk(self.expected_directories)

    @istest
    def failing(self):
        """Comparing wrong snapshot should fail.

        """
        self.loader.send_batch_revisions(self.in_revisions)
        with self.assertRaises(AssertionError):
            self.assertRevisionsOk([])


class LoadTestRelease(DummyBaseLoaderTest):
    def setUp(self):
        super().setUp()
        self.loader = LoaderNoStorage()

        self.release_id0 = '44e45d56f88993aae6a0198013efa80716fd8921'
        self.release_id1 = '54e45d56f88993aae6a0198013efa80716fd8920'
        self.release_id2 = '43e45d56f88993aae6a0198013efa80716fd8920'
        # trimmed data to the bare necessities
        self.in_releases = [{
            'id': hash_to_bytes(self.release_id0),
        }, {
            'id': hash_to_bytes(self.release_id1),
        }, {
            'id': hash_to_bytes(self.release_id2),
        }]
        self.expected_releases = [
            self.release_id0, self.release_id1, self.release_id2]

    @istest
    def maybe_load_releases(self):
        """Loading releases should be ok

        """
        self.loader.maybe_load_releases(self.in_releases)
        self.assertCountReleases(len(self.expected_releases))
        self.assertReleasesOk(self.expected_releases)

    @istest
    def send_batch_releases(self):
        """Sending releases should be ok 2

        """
        self.loader.send_batch_releases(self.in_releases)
        self.assertCountReleases(len(self.expected_releases))
        self.assertReleasesOk(self.expected_releases)

    @istest
    def failing(self):
        """Comparing wrong snapshot should fail.

        """
        self.loader.send_batch_releases(self.in_releases)
        with self.assertRaises(AssertionError):
            self.assertReleasesOk([])


class LoadTestRevision(DummyBaseLoaderTest):
    def setUp(self):
        super().setUp()
        self.loader = LoaderNoStorage()

        rev_id0 = '44e45d56f88993aae6a0198013efa80716fd8921'
        dir_id0 = '34973274ccef6ab4dfaaf86599792fa9c3fe4689'
        rev_id1 = '54e45d56f88993aae6a0198013efa80716fd8920'
        dir_id1 = '61c2b3a30496d329e21af70dd2d7e097046d07b7'
        rev_id2 = '43e45d56f88993aae6a0198013efa80716fd8920'
        dir_id2 = '33e45d56f88993aae6a0198013efa80716fd8921'

        # data trimmed to bare necessities
        self.in_revisions = [{
            'id': hash_to_bytes(rev_id0),
            'directory': hash_to_bytes(dir_id0),
        }, {
            'id': hash_to_bytes(rev_id1),
            'directory': hash_to_bytes(dir_id1),
        }, {
            'id': hash_to_bytes(rev_id2),
            'directory': hash_to_bytes(dir_id2),
        }]

        self.expected_revisions = {
            rev_id0: dir_id0,
            rev_id1: dir_id1,
            rev_id2: dir_id2,
        }

    @istest
    def maybe_load_revisions(self):
        """Loading revisions should be ok

        """
        self.loader.maybe_load_revisions(self.in_revisions)
        self.assertCountRevisions(len(self.expected_revisions))
        self.assertRevisionsOk(self.expected_revisions)

    @istest
    def send_batch_revisions(self):
        """Sending revisions should be ok 2

        """
        self.loader.send_batch_revisions(self.in_revisions)
        self.assertCountRevisions(len(self.expected_revisions))
        self.assertRevisionsOk(self.expected_revisions)

    @istest
    def failing(self):
        """Comparing wrong snapshot should fail.

        """
        self.loader.send_batch_revisions(self.in_revisions)
        with self.assertRaises(AssertionError):
            self.assertRevisionsOk([])


class LoadTestSnapshot(DummyBaseLoaderTest):
    def setUp(self):
        super().setUp()
        self.loader = LoaderNoStorage()

        snapshot_id = '44e45d56f88993aae6a0198013efa80716fd8921'
        revision_id = '54e45d56f88993aae6a0198013efa80716fd8920'
        release_id = '43e45d56f88993aae6a0198013efa80716fd8920'
        # trimmed data to the bare necessities
        self.expected_snapshot = {
            'id': snapshot_id,
            'branches': {
                'default': {
                    'target_type': 'revision',
                    'target': revision_id,
                },
                'master': {
                    'target_type': 'release',
                    'target': release_id,
                },
                'HEAD': {
                    'target_type': 'alias',
                    'target': 'master',
                }
            }
        }

        self.in_snapshot = {
            'id': hash_to_bytes(snapshot_id),
            'branches': {
                b'default': {
                    'target_type': 'revision',
                    'target': hash_to_bytes(revision_id),
                },
                b'master': {
                    'target_type': 'release',
                    'target': hash_to_bytes(release_id),
                },
                b'HEAD': {
                    'target_type': 'alias',
                    'target': b'master',
                }
            }
        }

    @istest
    def maybe_load_snapshots(self):
        """Loading snapshot should be ok

        """
        self.loader.maybe_load_snapshot(self.in_snapshot)
        self.assertCountSnapshots(1)
        self.assertSnapshotOk(self.expected_snapshot)
        self.assertSnapshotOk(
            self.expected_snapshot['id'],
            expected_branches=self.expected_snapshot['branches'])

    @istest
    def send_batch_snapshots(self):
        """Sending snapshot should be ok 2

        """
        self.loader.send_snapshot(self.in_snapshot)
        self.assertCountSnapshots(1)
        self.assertSnapshotOk(self.expected_snapshot)
        self.assertSnapshotOk(
            self.expected_snapshot['id'],
            expected_branches=self.expected_snapshot['branches'])

    @istest
    def failing(self):
        """Comparing wrong snapshot should fail.

        """
        self.loader.send_snapshot(self.in_snapshot)
        with self.assertRaises(AssertionError):
            self.assertSnapshotOk(
                'wrong', expected_branches=self.expected_snapshot['branches'])
