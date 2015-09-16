# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.loader.git.storage import db, models
from swh.loader.git.protocols import serial
from test_utils import now, app_client, app_client_teardown
from swh.core import hashutil

@attr('slow')
class TestPostObjectsPerTypeCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app, self.db_url, self.content_storage_dir = app_client()

        with db.connect(self.db_url) as db_conn:
            self.content_sha1_id = '09a8ca4c0f510fda04a4dfe04d842cdfaafa7d8c'
            self.content_sha1_id_bin = hashutil.hex_to_hash(self.content_sha1_id)

            self.content_sha256_bin = hashutil.hashdata(b'something-to-hash', ['sha256'])['sha256']
            models.add_content(db_conn,
                               self.content_sha1_id_bin,
                               self.content_sha1_id_bin,
                               self.content_sha256_bin,
                               10)

            self.directory_sha1_hex = '19a8ca4c0f510fda04a4dfe04d842cdfaafa7d8c'
            self.directory_sha1_bin = hashutil.hex_to_hash(self.directory_sha1_hex)
            models.add_directory(db_conn, self.directory_sha1_bin)

            authorAndCommitter = {'name': 'some-name', 'email': 'some-email'}
            models.add_person(db_conn, authorAndCommitter['name'], authorAndCommitter['email'])

            authorAndCommitter2 = {'name': 'tony', 'email': 'tony@dude.org'}
            models.add_person(db_conn, authorAndCommitter2['name'], authorAndCommitter2['email'])

            self.revision_sha1_hex = '29a8ca4c0f510fda04a4dfe04d842cdfaafa7d8c'
            self.revision_sha1_bin = hashutil.hex_to_hash(self.revision_sha1_hex)
            models.add_revision(db_conn,
                                self.revision_sha1_bin,
                                now(),
                                now(),
                                self.directory_sha1_bin,
                                "revision message",
                                authorAndCommitter,
                                authorAndCommitter)

            self.revision_sha1_hex2 = '39a8ca4c0f510fda04a4dfe04d842cdfaafa7d8c'
            self.revision_sha1_bin2 = hashutil.hex_to_hash(self.revision_sha1_hex2)
            models.add_revision(db_conn,
                                self.revision_sha1_bin2,
                                now(),
                                now(),
                                self.directory_sha1_bin,
                                "revision message",
                                authorAndCommitter2,
                                authorAndCommitter2,
                                parent_shas=['revision-sha1-to-test-existence9994f717e'])

            self.release_sha1_hex = '49a8ca4c0f510fda04a4dfe04d842cdfaafa7d8c'
            self.release_sha1_bin = hashutil.hex_to_hash(self.release_sha1_hex)
            models.add_release(db_conn,
                               self.release_sha1_bin,
                               self.revision_sha1_bin,
                               now(),
                               "0.0.1",
                               "Super release tagged by tony",
                               authorAndCommitter2)

            self.origin_url = "https://github.com/user/repo"
            models.add_origin(db_conn, self.origin_url, 'git')

            models.add_occurrence_history(db_conn,
                                          self.origin_url,
                                          'master',
                                          self.revision_sha1_bin,
                                          'softwareheritage')

    @classmethod
    def tearDownClass(self):
        app_client_teardown(self.content_storage_dir)

    @istest
    def post_all_non_presents_contents(self):
        # given

        # when
        payload = [self.content_sha1_id_bin,
                   hashutil.hex_to_hash('555444f9dd5dc46ee476a8be155ab049994f717e'),
                   hashutil.hex_to_hash('555444f9dd5dc46ee476a8be155ab049994f717e'),
                   hashutil.hex_to_hash('666777f9dd5dc46ee476a8be155ab049994f717e')]
        query_payload = serial.dumps(payload)

        rv = self.app.post('/vcs/contents/',
                           data=query_payload,
                           headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 200

        sha1s = serial.loads(rv.data)
        assert len(sha1s) is 2                                     # only 2 sha1s
        assert hashutil.hex_to_hash("666777f9dd5dc46ee476a8be155ab049994f717e") in sha1s
        assert hashutil.hex_to_hash("555444f9dd5dc46ee476a8be155ab049994f717e") in sha1s

    @istest
    def post_all_non_presents_directories(self):
        # given

        # when
        payload = [self.directory_sha1_bin,
                   hashutil.hex_to_hash('555444f9dd5dc46ee476a8be155ab049994f717e'),
                   hashutil.hex_to_hash('555444f9dd5dc46ee476a8be155ab049994f717e'),
                   hashutil.hex_to_hash('666777f9dd5dc46ee476a8be155ab049994f717e')]
        query_payload = serial.dumps(payload)

        rv = self.app.post('/vcs/directories/',
                           data=query_payload,
                           headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 200

        sha1s = serial.loads(rv.data)
        assert len(sha1s) is 2                                     # only 2 sha1s
        assert hashutil.hex_to_hash("666777f9dd5dc46ee476a8be155ab049994f717e") in sha1s
        assert hashutil.hex_to_hash("555444f9dd5dc46ee476a8be155ab049994f717e") in sha1s

    @istest
    def post_all_non_presents_revisions(self):
        # given

        # when
        payload = [self.revision_sha1_bin,
                   self.revision_sha1_bin,
                   hashutil.hex_to_hash('555444f9dd5dc46ee476a8be155ab049994f717e'),
                   hashutil.hex_to_hash('555444f9dd5dc46ee476a8be155ab049994f717e'),
                   hashutil.hex_to_hash('666777f9dd5dc46ee476a8be155ab049994f717e')]
        query_payload = serial.dumps(payload)

        rv = self.app.post('/vcs/revisions/',
                           data=query_payload,
                           headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 200

        sha1s = serial.loads(rv.data)
        assert len(sha1s) is 2                                     # only 2 sha1s
        assert hashutil.hex_to_hash("666777f9dd5dc46ee476a8be155ab049994f717e") in sha1s
        assert hashutil.hex_to_hash("555444f9dd5dc46ee476a8be155ab049994f717e") in sha1s

    @istest
    def post_all_non_presents_releases(self):
        # given

        # when
        payload = [self.release_sha1_bin,
                   self.release_sha1_bin,
                   hashutil.hex_to_hash('555444f9dd5dc46ee476a8be155ab049994f717e'),
                   hashutil.hex_to_hash('555444f9dd5dc46ee476a8be155ab049994f717e'),
                   hashutil.hex_to_hash('666777f9dd5dc46ee476a8be155ab049994f717e')]
        query_payload = serial.dumps(payload)

        rv = self.app.post('/vcs/releases/',
                           data=query_payload,
                           headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 400
        assert rv.data  == b'Bad request. Type not supported!'

    @istest
    def post_all_non_presents_occurrences_KO(self):
        # given

        # when
        payload = [self.revision_sha1_bin,
                   self.revision_sha1_bin,
                   hashutil.hex_to_hash('555444f9dd5dc46ee476a8be155ab049994f717e'),
                   hashutil.hex_to_hash('555444f9dd5dc46ee476a8be155ab049994f717e'),
                   hashutil.hex_to_hash('666777f9dd5dc46ee476a8be155ab049994f717e')]
        query_payload = serial.dumps(payload)

        rv = self.app.post('/vcs/occurrences/',
                           data=query_payload,
                           headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 400
        assert rv.data  == b'Bad request. Type not supported!'

    @istest
    def post_non_presents_objects_empty_payload_so_empty_results(self):
        # given

        # when
        for api_type in ['contents', 'directories', 'revisions']:
            rv = self.app.post('/vcs/%s/' % api_type,
                               data=serial.dumps({}),
                               headers={'Content-Type': serial.MIMETYPE})

            # then
            assert rv.status_code == 200
            assert serial.loads(rv.data) == []

    @istest
    def post_non_presents_objects_bad_requests_format_pickle(self):
        # given

        # when
        for api_type in ['contents', 'directories', 'revisions']:
            rv = self.app.post('/vcs/%s/' % api_type,
                               data="not pickle -> fail")

            # then
            assert rv.status_code == 400
            assert rv.data == b'Bad request. Expected application/octet-stream data!'
