# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.storage import db, models
from swh.protocols import serial
from test_utils import now, app_client


@attr('slow')
class RevisionTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        with db.connect(db_url) as db_conn:
            self.directory_sha1_hex = 'directory-sha16ee476a8be155ab049994f717e'
            models.add_directory(db_conn, self.directory_sha1_hex)

            self.revision_sha1_hex = 'revision-sha1-to-test-existence9994f717e'
            models.add_revision(db_conn,
                                self.revision_sha1_hex,
                                now(),
                                self.directory_sha1_hex,
                                "revision message",
                                "ardumont",
                                "ardumont")

    @istest
    def get_revision_ok(self):
        # when
        rv = self.app.get('/vcs/revisions/%s' % self.revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == self.revision_sha1_hex

    @istest
    def get_revision_not_found(self):
        # when
        rv = self.app.get('/vcs/revisions/inexistant-sha1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_revision_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/vcs/revisions/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_revision_create_and_update(self):
        revision_sha1_hex = 'sha1-revision46ee476a8be155ab049994f717e'

        rv = self.app.get('/vcs/revisions/%s' % revision_sha1_hex)

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        body = serial.dumps({'content': b'revision has content too.',
                             'date': now(),
                             'directory': self.directory_sha1_hex,
                             'message': 'revision message describing it',
                             'committer': 'ardumont',
                             'author': 'ardumont',
                             'parent-sha1s': [self.revision_sha1_hex]})

        rv = self.app.put('/vcs/revisions/%s' % revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/revisions/%s' % revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == revision_sha1_hex

        # we update it
        rv = self.app.put('/vcs/revisions/%s' % revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/vcs/revisions/%s' % revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == revision_sha1_hex
