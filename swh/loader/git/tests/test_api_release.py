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


@attr('slow')
class ReleaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app, db_url, self.content_storage_dir = app_client()

        with db.connect(db_url) as db_conn:
            self.directory_sha1_hex = 'directory-sha16ee476a8be155ab049994f717e'
            models.add_directory(db_conn, self.directory_sha1_hex)

            self.tagAuthor = {'name': 'tony', 'email': 'tony@mail.org'}
            models.add_person(db_conn, self.tagAuthor['name'], self.tagAuthor['email'])

            self.revision_sha1_hex = 'revision-sha1-to-test-existence9994f717e'
            models.add_revision(db_conn,
                                self.revision_sha1_hex,
                                now(),
                                now(),
                                self.directory_sha1_hex,
                                "revision message",
                                self.tagAuthor,
                                self.tagAuthor)

            self.release_sha1_hex = 'release-sha1-to-test-existence1234567901'
            models.add_release(db_conn,
                               self.release_sha1_hex,
                               self.revision_sha1_hex,
                               now(),
                               "0.0.1",
                               "Super release tagged by tony",
                               self.tagAuthor)
    @classmethod
    def tearDownClass(self):
        app_client_teardown(self.content_storage_dir)

    @istest
    def get_release_ok(self):
        # when
        rv = self.app.get('/vcs/releases/%s' % self.release_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == self.release_sha1_hex

    @istest
    def get_release_not_found(self):
        # when
        rv = self.app.get('/vcs/releases/inexistant-sha1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_release_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/vcs/releases/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_release_create_and_update(self):
        release_sha1_hex = 'sha1-release46ee476a8be155ab049994f717e'

        rv = self.app.get('/vcs/releases/%s' % release_sha1_hex)

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        body = serial.dumps({'id': release_sha1_hex,
                             'revision': self.revision_sha1_hex,
                             'date': now(),
                             'name': '0.0.1',
                             'comment': 'super release tagged by ardumont',
                             'author': self.tagAuthor})

        rv = self.app.put('/vcs/releases/%s' % release_sha1_hex,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/releases/%s' % release_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == release_sha1_hex

        # we update it
        rv = self.app.put('/vcs/releases/%s' % release_sha1_hex,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # still the same
        rv = self.app.get('/vcs/releases/%s' % release_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == release_sha1_hex
