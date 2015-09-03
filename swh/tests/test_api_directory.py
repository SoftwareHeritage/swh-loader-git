# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.store import db, models
from swh.protocols import serial
from test_utils import now, app_client


@attr('slow')
class DirectoryTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        with db.connect(db_url) as db_conn:
            self.content_sha1_id = 'content-sha1c46ee476a8be155ab049994f717e'
            content_sha1_hex = 'content-sha1c46ee476a8be155ab049994f717e'
            content_sha256_hex = 'content-sha2566ee476a8be155ab049994f717e'
            models.add_content(db_conn,
                               self.content_sha1_id,
                               content_sha1_hex,
                               content_sha256_hex,
                               10)

            self.directory_sha1_hex = 'directory-sha16ee476a8be155ab049994f717e'
            models.add_directory(db_conn, self.directory_sha1_hex)

    @istest
    def get_directory_ok(self):
        # when
        rv = self.app.get('/vcs/directories/%s' % self.directory_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == 'directory-sha16ee476a8be155ab049994f717e'

    @istest
    def get_directory_not_found(self):
        # when
        rv = self.app.get('/vcs/directories/111111f9dd5dc46ee476a8be155ab049994f7170')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_directory_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/vcs/directories/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_directory_create_and_update(self):
        directory_sha1='directory-sha16ee476a8be155ab049994f7170'

        # does not exist
        rv = self.app.get('/vcs/directories/%s' % directory_sha1)

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        body = serial.dumps({'content': b'directory has content too.',
                             'entries': [{'name': 'filename',
                                          'target-sha1': self.content_sha1_id,
                                          'nature': 'file',
                                          'perms': '000',
                                          'atime': now(),
                                          'mtime': now(),
                                          'ctime': now(),
                                          'parent': directory_sha1},
                                         {'name': 'dirname',
                                          'target-sha1': self.directory_sha1_hex,
                                          'nature': 'directory',
                                          'perms': '012',
                                          'atime': now(),
                                          'mtime': now(),
                                          'ctime': now(),
                                          'parent': directory_sha1}
                                     ]})

        rv = self.app.put('/vcs/directories/%s' % directory_sha1,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/directories/%s' % directory_sha1)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == 'directory-sha16ee476a8be155ab049994f7170'

        # we update it
        rv = self.app.put('/vcs/directories/directory-sha16ee476a8be155ab049994f7170',
                          data=serial.dumps({'entry': 'directory-bar'}),
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/vcs/directories/directory-sha16ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == 'directory-sha16ee476a8be155ab049994f7170'
