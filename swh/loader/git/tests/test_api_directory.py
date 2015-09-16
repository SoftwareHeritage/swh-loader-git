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
from swh.core import hashutil

@attr('slow')
class DirectoryTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app, db_url, self.content_storage_dir = app_client()

        with db.connect(db_url) as db_conn:
            self.content_sha1_id = hashutil.hex_to_hash('e5ba97de299a0e1e26b4a471b3d67c098d178e6e')
            content_sha1_bin = hashutil.hex_to_hash('d5ba97de299a0e1e26b4a471b3d67c098d178e6e')
            content_sha256_bin = hashutil.hashdata(b'something-to-hash', ['sha256'])['sha256']
            models.add_content(db_conn,
                               self.content_sha1_id,
                               content_sha1_bin,
                               content_sha256_bin,
                               10)

            self.directory_sha1_hex = 'b5ba97de299a0e1e26b4a471b3d67c098d178e6e'
            directory_sha1_bin = hashutil.hex_to_hash(self.directory_sha1_hex)
            models.add_directory(db_conn, directory_sha1_bin)

            self.directory_sha1_put = 'a5ba97de299a0e1e26b4a471b3d67c098d178e6e'
            self.directory_sha1_put_bin = hashutil.hex_to_hash(self.directory_sha1_put)
            models.add_directory(db_conn, self.directory_sha1_put_bin)

    @classmethod
    def tearDownClass(self):
        app_client_teardown(self.content_storage_dir)

    @istest
    def get_directory_ok(self):
        # when
        rv = self.app.get('/vcs/directories/%s' % self.directory_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == self.directory_sha1_hex

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
        directory_sha1 = '15ba97de299a0e1e26b4a471b3d67c098d178e6e'

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
                                             'target-sha1': self.directory_sha1_put_bin,
                                             'perms': '012',
                                             'atime': None,
                                             'mtime': None,
                                             'ctime': None}],
                             'entry-revs': [{'name': "rev-name",
                                             'type': storage.Type.directory_entry,
                                             'target-sha1': hashutil.hex_to_hash('35ba97de299a0e1e26b4a471b3d67c098d178e6e'),
                                             'perms': '000',
                                             'atime': None,
                                             'mtime': None,
                                             'ctime': None}]
        })

        rv = self.app.put('/vcs/directories/%s' % directory_sha1,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        print(rv.status_code)
        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/directories/%s' % directory_sha1)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == directory_sha1

        # we update it
        rv = self.app.put('/vcs/directories/%s' % directory_sha1,
                          data=serial.dumps({'entry-files': 'directory-bar'}),
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # still the same
        rv = self.app.get('/vcs/directories/%s' % directory_sha1)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == directory_sha1
