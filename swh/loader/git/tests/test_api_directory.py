# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.loader.git.storage import db, models, storage
from swh.loader.git.protocols import serial
from test_utils import app_client, app_client_teardown

@attr('slow')
class DirectoryTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app, db_url, self.content_storage_dir = app_client()

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

            self.directory_sha1_put = 'directory-sha36ee476a8be155ab049994f717e'
            models.add_directory(db_conn, self.directory_sha1_put)

    @classmethod
    def tearDownClass(self):
        app_client_teardown(self.content_storage_dir)

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
        body = serial.dumps({'entry-files': [{'name': 'filename',
                                              'type': storage.Type.directory_entry,
                                              'target-sha1': self.content_sha1_id,
                                              'perms': '000',
                                              'atime': None,
                                              'mtime': None,
                                              'ctime': None}],
                             'entry-dirs': [{'name': 'dirname',
                                             'type': storage.Type.directory_entry,
                                             'target-sha1': self.directory_sha1_put,
                                             'perms': '012',
                                             'atime': None,
                                             'mtime': None,
                                             'ctime': None}],
                             'entry-revs': [{'name': "rev-name",
                                             'type': storage.Type.directory_entry,
                                             'target-sha1': 'git-submodule-inexistant',
                                             'perms': '000',
                                             'atime': None,
                                             'mtime': None,
                                             'ctime': None}]
        })

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
                          data=serial.dumps({'entry-files': 'directory-bar'}),
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # still the same
        rv = self.app.get('/vcs/directories/directory-sha16ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == 'directory-sha16ee476a8be155ab049994f7170'
