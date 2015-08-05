# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import json

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.storage import db, models

from test_utils import now, app_client


@attr('slow')
class TestObjectsPerTypeCase(unittest.TestCase):
    def setUp(self):
        self.app, self.db_url = app_client()

        with db.connect(self.db_url) as db_conn:
            self.content_sha1_id = 'sha1-content0-6ee476a8be155ab049994f717e'
            self.content_sha256_hex = 'sha256-content0-e476a8be155ab049994f717e'
            models.add_content(db_conn,
                               self.content_sha1_id,
                               self.content_sha1_id,
                               self.content_sha256_hex,
                               10)

            self.directory_sha1_hex = 'directory-sha1-ee476a8be155ab049994f717e'
            models.add_directory(db_conn, self.directory_sha1_hex)

            self.revision_sha1_hex = 'revision-sha1-to-test-existence9994f717e'
            models.add_revision(db_conn,
                                self.revision_sha1_hex,
                                now(),
                                self.directory_sha1_hex,
                                "revision message",
                                "ardumont",
                                "ardumont")

            self.revision_sha1_hex2 = 'revision-sha1-2-for-testing-put-occurr'
            models.add_revision(db_conn,
                                self.revision_sha1_hex2,
                                now(),
                                self.directory_sha1_hex,
                                "revision message",
                                "ardumont",
                                "ardumont",
                                parent_shas=['revision-sha1-to-test-existence9994f717e'])

            self.release_sha1_hex = 'release-sha1-to-test-existence1234567901'
            models.add_release(db_conn,
                               self.release_sha1_hex,
                               self.revision_sha1_hex,
                               now(),
                               "0.0.1",
                               "Super release tagged by tony",
                               "tony")

            self.origin_url = "https://github.com/user/repo"
            models.add_origin(db_conn, self.origin_url, 'git')

            models.add_occurrence(db_conn,
                                  self.origin_url,
                                  'master',
                                  self.revision_sha1_hex)

    @istest
    def post_all_non_presents_contents(self):
        # given

        # when
        payload = {'sha1s': [self.content_sha1_id,
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '666777f9dd5dc46ee476a8be155ab049994f717e']}
        json_payload = json.dumps(payload)

        rv = self.app.post('/vcs/contents/',
                           data=json_payload,
                           headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 200

        json_result = json.loads(rv.data.decode('utf-8'))
        assert len(json_result.keys()) is 1                                       # only 1 key
        assert len(json_result['sha1s']) is 2                                     # only 2 sha1s
        sha1s = json_result['sha1s']
        assert "666777f9dd5dc46ee476a8be155ab049994f717e" in sha1s
        assert "555444f9dd5dc46ee476a8be155ab049994f717e" in sha1s

    @istest
    def post_all_non_presents_directories(self):
        # given

        # when
        payload = {'sha1s': [self.directory_sha1_hex,
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '666777f9dd5dc46ee476a8be155ab049994f717e']}
        json_payload = json.dumps(payload)

        rv = self.app.post('/vcs/directories/',
                           data=json_payload,
                           headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 200

        json_result = json.loads(rv.data.decode('utf-8'))
        assert len(json_result.keys()) is 1                                       # only 1 key
        assert len(json_result['sha1s']) is 2                                     # only 2 sha1s
        sha1s = json_result['sha1s']
        assert "666777f9dd5dc46ee476a8be155ab049994f717e" in sha1s
        assert "555444f9dd5dc46ee476a8be155ab049994f717e" in sha1s

    @istest
    def post_all_non_presents_revisions(self):
        # given

        # when
        payload = {'sha1s': [self.revision_sha1_hex,
                             self.revision_sha1_hex,
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '666777f9dd5dc46ee476a8be155ab049994f717e']}
        json_payload = json.dumps(payload)

        rv = self.app.post('/vcs/revisions/',
                           data=json_payload,
                           headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 200

        json_result = json.loads(rv.data.decode('utf-8'))
        assert len(json_result.keys()) is 1                                       # only 1 key
        assert len(json_result['sha1s']) is 2                                     # only 2 sha1s
        sha1s = json_result['sha1s']
        assert "666777f9dd5dc46ee476a8be155ab049994f717e" in sha1s
        assert "555444f9dd5dc46ee476a8be155ab049994f717e" in sha1s

    @istest
    def post_all_non_presents_releases(self):
        # given

        # when
        payload = {'sha1s': [self.release_sha1_hex,
                             self.release_sha1_hex,
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '666777f9dd5dc46ee476a8be155ab049994f717e']}
        json_payload = json.dumps(payload)

        rv = self.app.post('/vcs/releases/',
                           data=json_payload,
                           headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 200

        json_result = json.loads(rv.data.decode('utf-8'))
        assert len(json_result.keys()) is 1                                       # only 1 key
        assert len(json_result['sha1s']) is 2                                     # only 2 sha1s
        sha1s = json_result['sha1s']
        assert "666777f9dd5dc46ee476a8be155ab049994f717e" in sha1s
        assert "555444f9dd5dc46ee476a8be155ab049994f717e" in sha1s

    @istest
    def post_all_non_presents_occurrences_KO(self):
        # given

        # when
        payload = {'sha1s': [self.revision_sha1_hex,
                             self.revision_sha1_hex,
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '666777f9dd5dc46ee476a8be155ab049994f717e']}
        json_payload = json.dumps(payload)

        rv = self.app.post('/vcs/occurrences/',
                           data=json_payload,
                           headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 400
        assert rv.data  == b'Bad request. Type not supported!'

    @istest
    def post_non_presents_objects_bad_requests(self):
        # given

        # when
        rv = self.app.post('/vcs/releases/',
                           data=json.dumps({}),
                           headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 400
