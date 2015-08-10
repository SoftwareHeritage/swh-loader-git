# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.protocols import serial
from swh.storage import db, models
from test_utils import now, app_client


@attr('slow')
class TestObjectsCase(unittest.TestCase):
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
    def get_non_presents_objects(self):
        # given

        # when
        payload = [self.content_sha1_id,
                   self.directory_sha1_hex,
                   self.revision_sha1_hex,
                   self.revision_sha1_hex,
                   self.release_sha1_hex,
                   '555444f9dd5dc46ee476a8be155ab049994f717e',
                   '555444f9dd5dc46ee476a8be155ab049994f717e',
                   '666777f9dd5dc46ee476a8be155ab049994f717e']
        query_payload = serial.dumps(payload)

        rv = self.app.post('/objects/',
                           data=query_payload,
                           headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 200

        sha1s = serial.loads(rv.data)
        assert len(sha1s) is 2                                     # only 2 sha1s
        assert "666777f9dd5dc46ee476a8be155ab049994f717e" in sha1s
        assert "555444f9dd5dc46ee476a8be155ab049994f717e" in sha1s

    @istest
    def get_non_presents_objects_empty_payload_empty_result(self):
        # given

        # when
        rv = self.app.post('/objects/',
                           data=serial.dumps({}),
                           headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data) == []

    @istest
    def put_non_presents_objects(self):
        content_sha1_unknown1 = 'content-sha1-46ee476a8be155ab049994f717e'
        content_sha1_unknown2 = 'content-sha1-2-ee476a8be155ab049994f717e'
        directory_sha1_unknown = 'directory-sha1-46ee476a8be155ab049994f717e'
        revision_sha1_unknown = 'revision-sha1-46ee476a8be155ab049994f717e'
        revision_sha1_unknown2 = 'revision-sha1-2-46ee476a8be155ab049994f717e'
        release_sha1_unknown = 'release-sha1-46ee476a8be155ab049994f717e'

        # given
        payload_1 = [self.content_sha1_id,
                     self.directory_sha1_hex,
                     self.revision_sha1_hex,
                     self.revision_sha1_hex,
                     self.release_sha1_hex,
                     content_sha1_unknown1,
                     content_sha1_unknown1,  # duplicates is not a concern
                     content_sha1_unknown2,
                     directory_sha1_unknown,
                     revision_sha1_unknown,
                     revision_sha1_unknown2,
                     release_sha1_unknown]

        query_payload_1 = serial.dumps(payload_1)

        rv = self.app.post('/objects/',
                           data=query_payload_1,
                           headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 200

        sha1s = serial.loads(rv.data)
        assert len(sha1s) is 6                                      # only 6 sha1s
        assert content_sha1_unknown1 in sha1s
        assert content_sha1_unknown2 in sha1s
        assert directory_sha1_unknown in sha1s
        assert release_sha1_unknown in sha1s
        assert revision_sha1_unknown in sha1s
        assert revision_sha1_unknown2 in sha1s

        # when
        payload_contents = [{'sha1': content_sha1_unknown1,
                             'content-sha1': 'content-sha1c46ee476a8be155ab03333333333',
                             'content-sha256': 'content-sha2566ee476a8be155ab03333333333',
                             'content': b'bar',
                             'size': '3'},
                            {'sha1': content_sha1_unknown2,
                             'content-sha1': '555444f9dd5dc46ee476a8be155ab049994f717e',
                             'content-sha256': '555444f9dd5dc46ee476a8be155ab049994f717e',
                             'content': b'foobar',
                             'size': 6}]
        query_payload_contents = serial.dumps(payload_contents)

        rv = self.app.put('/vcs/contents/',
                          data=query_payload_contents,
                          headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 204

        # Sent back the first requests and see that we now have less unknown
        # sha1s (no more missed contents )
        rv = self.app.post('/objects/', data=query_payload_1,
                           headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 200

        sha1s = serial.loads(rv.data)
        assert len(sha1s) is 4                                      # only 6 sha1s
        assert directory_sha1_unknown in sha1s
        assert release_sha1_unknown in sha1s
        assert revision_sha1_unknown in sha1s
        assert revision_sha1_unknown2 in sha1s

        # when
        payload_directories = [{'sha1': directory_sha1_unknown,
                                'content': b'directory has content too.',
                                'entries': [{'name': 'filename',
                                             'target-sha1': self.content_sha1_id,
                                             'nature': 'file',
                                             'perms': '000',
                                             'atime': now(),
                                             'mtime': now(),
                                             'ctime': now(),
                                             'parent': directory_sha1_unknown},
                                            {'name': 'dirname',
                                             'target-sha1': self.directory_sha1_hex,
                                             'nature': 'directory',
                                             'perms': '012',
                                             'atime': now(),
                                             'mtime': now(),
                                             'ctime': now(),
                                             'parent': directory_sha1_unknown}]
        }]

        query_payload_directories = serial.dumps(payload_directories)

        rv = self.app.put('/vcs/directories/',
                          data=query_payload_directories,
                          headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 204

        # Sent back the first requests and see that we now have less unknown
        # sha1s (no more missed directories)
        rv = self.app.post('/objects/',
                           data=query_payload_1,
                           headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 200

        sha1s = serial.loads(rv.data)
        assert len(sha1s) is 3                                      # only 1 sha1 unknown
        assert release_sha1_unknown in sha1s
        assert revision_sha1_unknown in sha1s
        assert revision_sha1_unknown2 in sha1s

        # when
        payload_releases = [{'sha1': release_sha1_unknown,
                             'content': b'release also has content',
                             'revision': self.revision_sha1_hex,
                             'date': now(),
                             'name': '0.0.1',
                             'comment': 'super release tagged by ardumont',
                             'author': 'ardumont'},
                            {'sha1': 'another-sha1',
                             'content': b'some content',
                             'revision': self.revision_sha1_hex,
                             'date': now(),
                             'name': '0.0.2',
                             'comment': 'fix bugs release by zack and olasd',
                             'author': 'the Dude'}]

        query_payload_releases = serial.dumps(payload_releases)

        rv = self.app.put('/vcs/releases/',
                          data=query_payload_releases,
                          headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 204

        # Sent back the first requests and see that we now have less unknown
        # sha1s (no more missed directories)
        rv = self.app.post('/objects/',
                           data=query_payload_1,
                           headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 200

        sha1s = serial.loads(rv.data)
        assert len(sha1s) is 2
        assert revision_sha1_unknown in sha1s
        assert revision_sha1_unknown2 in sha1s

        # when
        payload_revisions = [{'sha1': revision_sha1_unknown,
                              'content': b'some content',
                              'date': now(),
                              'directory': directory_sha1_unknown,
                              'message': "commit message",
                              'author': "author",
                              'committer': "committer",
                              'parent-sha1s': []},
                             {'sha1': revision_sha1_unknown2,
                              'content': b'some other content',
                              'date': now(),
                              'directory': directory_sha1_unknown,
                              'message': "some other commit message",
                              'author': "author",
                              'committer': "committer",
                              'parent-sha1s': []},
        ]

        query_payload_revisions = serial.dumps(payload_revisions)

        rv = self.app.put('/vcs/revisions/',
                          data=query_payload_revisions,
                          headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 204

        # Sent back the first requests and see that we now have less unknown
        # sha1s (no more missed directories)
        rv = self.app.post('/objects/',
                           data=query_payload_1,
                           headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 200

        sha1s = serial.loads(rv.data)
        assert len(sha1s) is 0


    @istest
    def put_all_occurrences(self):
        # when
        rv = self.app.get('/vcs/occurrences/%s' % self.revision_sha1_hex2)
        # then
        assert rv.status_code == 404

        # when
        payload_occurrences = [{'sha1': self.revision_sha1_hex2,
                                'content': b'some content',
                                'reference': 'master',
                                'url-origin': self.origin_url},
                               {'sha1': self.revision_sha1_hex2,
                                'content': b'some content',
                                'reference': 'puppets',
                                'url-origin': self.origin_url}]

        query_payload_occurrences = serial.dumps(payload_occurrences)

        rv = self.app.put('/vcs/occurrences/',
                          data=query_payload_occurrences,
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204

        # when
        rv = self.app.get('/vcs/occurrences/%s' % self.revision_sha1_hex2)
        # then
        assert rv.status_code == 200