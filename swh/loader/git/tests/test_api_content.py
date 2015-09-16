# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.loader.git.storage import db, models
from swh.loader.git.protocols import serial
from test_utils import app_client, app_client_teardown
from swh.core import hashutil

@attr('slow')
class ContentTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app, db_url, self.content_storage_dir = app_client()

        with db.connect(db_url) as db_conn:
            self.content_sha1_id = '37bfdafafe3b16d970125df29e6ec9a3d2521fd2'
            self.content_sha1_id_bin = hashutil.hex_to_hash(self.content_sha1_id)

            content_sha1_id = '47bfdafafe3b16d970125df29e6ec9a3d2521fd2'
            content_sha1_id_bin = hashutil.hex_to_hash(content_sha1_id)

            self.content_sha256_bin = hashutil.hashdata(b'something-to-hash', ['sha256'])['sha256']
            models.add_content(db_conn,
                               self.content_sha1_id_bin,
                               content_sha1_id_bin,
                               self.content_sha256_bin,
                               10)

    @classmethod
    def tearDownClass(self):
        app_client_teardown(self.content_storage_dir)

    @istest
    def get_content_ok(self):
        # when
        rv = self.app.get('/vcs/contents/%s' % self.content_sha1_id)

        # then
        assert rv.status_code == 200
        data = serial.loads(rv.data)
        assert data['id'] == self.content_sha1_id

    @istest
    def get_content_not_found(self):
        # when
        rv = self.app.get('/vcs/contents/222222f9dd5dc46ee476a8be155ab049994f7170')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_content_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/vcs/contents/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_content_create_and_update(self):
        content_sha1 = '57bfdafafe3b16d970125df29e6ec9a3d2521fd2'
        content_git_sha1 = hashutil.hex_to_hash('57bfdafafe3b16d970125df29e6ec9a3d2521fd2')
        content_sha1_bin = hashutil.hex_to_hash(content_sha1)
        content_sha256_bin = hashutil.hashdata(b'another-thing-to-hash', ['sha256'])['sha256']

        # does not exist
        rv = self.app.get('/vcs/contents/%s' % content_sha1)

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        body = {'id': content_sha1_bin,
                'git-sha1': content_git_sha1,
                'content-sha256': content_sha256_bin,
                'content': b'bar',
                'size': '3'}

        rv = self.app.put('/vcs/contents/%s' % content_sha1,
                          data=serial.dumps(body),
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/contents/%s' % content_sha1)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == content_sha1

        # we update it
        rv = self.app.put('/vcs/contents/%s' % content_sha1,
                          data=serial.dumps(body),
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # still the same
        rv = self.app.get('/vcs/contents/%s' % content_sha1)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == content_sha1
