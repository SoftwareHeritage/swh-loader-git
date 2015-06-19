# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.backend import back
from swh import hash
from swh.storage import db, models

import test_initdb


def app_client(db_url="dbname=swhgitloader-test"):
    """Setup the application ready for testing.
    """
    back.app.config['conf'] = {'db_url': db_url,
                               'file_content_storage_dir': 'swh-git-loader/file-content-storage',
                               'object_content_storage_dir': 'swh-git-loader/object-content-storage',
                               'folder_depth': 2,
                               'blob_compression': None}
    back.app.config['TESTING'] = True
    app = back.app.test_client()
    test_initdb.prepare_db(db_url)
    return app, db_url


@attr('slow')
class HomeTestCase(unittest.TestCase):
    def setUp(self):
        self.app, _ = app_client()

    @istest
    def get_slash(self):
        # when
        rv = self.app.get('/')

        # then
        assert rv.status_code is 200
        assert rv.data == b'Dev SWH API'


@attr('slow')
class CommitTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        self.commit_sha1_hex = '000000f6dd5dc46ee476a8be155ab049994f717e'
        self.commit_sha1_bin = hash.sha1_bin(self.commit_sha1_hex)
        with db.connect(db_url) as db_conn:
            models.add_object(db_conn, self.commit_sha1_bin, models.Type.commit)

    @istest
    def get_commit_ok(self):
        # when
        rv = self.app.get('/commits/%s' % self.commit_sha1_hex)

        # then
        assert rv.status_code is 200
        assert rv.data == b'{\n  "sha1": "000000f6dd5dc46ee476a8be155ab049994f717e"\n}'

    @istest
    def get_commit_not_found(self):
        # when
        rv = self.app.get('/commits/000000f6dd5dc46ee476a8be155ab049994f7170')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_commit_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/commits/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_commit_create_and_update(self):
        # does not exist
        rv = self.app.get('/commits/000000f6dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        rv = self.app.put('/commits/000000f6dd5dc46ee476a8be155ab049994f7170', data = {'content': 'commit-foo'})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/commits/000000f6dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "000000f6dd5dc46ee476a8be155ab049994f7170"\n}'

        # we update it
        rv = self.app.put('/commits/000000f6dd5dc46ee476a8be155ab049994f7170', data = {'content': 'commit-foo'})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/commits/000000f6dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "000000f6dd5dc46ee476a8be155ab049994f7170"\n}'


@attr('slow')
class TreeTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        self.tree_sha1_hex = '111111f9dd5dc46ee476a8be155ab049994f717e'
        self.tree_sha1_bin = hash.sha1_bin(self.tree_sha1_hex)
        with db.connect(db_url) as db_conn:
            models.add_object(db_conn, self.tree_sha1_bin, models.Type.tree)

    @istest
    def get_tree_ok(self):
        # when
        rv = self.app.get('/trees/%s' % self.tree_sha1_hex)

        # then
        assert rv.status_code is 200
        assert rv.data == b'{\n  "sha1": "111111f9dd5dc46ee476a8be155ab049994f717e"\n}'

    @istest
    def get_tree_not_found(self):
        # when
        rv = self.app.get('/trees/111111f9dd5dc46ee476a8be155ab049994f7170')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_tree_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/trees/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_tree_create_and_update(self):
        # does not exist
        rv = self.app.get('/trees/111111f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        rv = self.app.put('/trees/111111f9dd5dc46ee476a8be155ab049994f7170', data = {'content': 'tree-bar'})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/trees/111111f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "111111f9dd5dc46ee476a8be155ab049994f7170"\n}'

        # we update it
        rv = self.app.put('/trees/111111f9dd5dc46ee476a8be155ab049994f7170', data = {'content': 'tree-bar'})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/trees/111111f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "111111f9dd5dc46ee476a8be155ab049994f7170"\n}'


@attr('slow')
class BlobTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        self.blob_sha1_hex = '222222f9dd5dc46ee476a8be155ab049994f717e'
        self.blob_sha1_bin = hash.sha1_bin(self.blob_sha1_hex)
        blog_git_sha1 = hash.sha1_bin('22222200000011111176a8be155ab049994f717e')
        with db.connect(db_url) as db_conn:
            models.add_blob(db_conn, self.blob_sha1_bin, 10, blog_git_sha1)

    @istest
    def get_blob_ok(self):
        # when
        rv = self.app.get('/blobs/%s' % self.blob_sha1_hex)

        # then
        assert rv.status_code is 200
        assert rv.data == b'{\n  "sha1": "222222f9dd5dc46ee476a8be155ab049994f717e"\n}'

    @istest
    def get_blob_not_found(self):
        # when
        rv = self.app.get('/blobs/222222f9dd5dc46ee476a8be155ab049994f7170')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_blob_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/blobs/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_blob_bad_request_bad_payload(self):
        # when
        # we create it
        rv = self.app.put('/blobs/222222f9dd5dc46ee476a8be155ab049994f7170',
                          data = {'size': 99,
                                  'git-sha1': 'bad-payload',
                                  'content': 'foo'})

        # then
        assert rv.status_code == 400
        assert rv.data == b'Bad request!'

    @istest
    def put_blob_create_and_update(self):
        # does not exist
        rv = self.app.get('/blobs/222222f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        rv = self.app.put('/blobs/222222f9dd5dc46ee476a8be155ab049994f7170',
                          data = {'size': 99,
                                  'git-sha1': '222222f9dd5dc46ee476a8be155ab03333333333',
                                  'content': 'bar'})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/blobs/222222f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "222222f9dd5dc46ee476a8be155ab049994f7170"\n}'

        # we update it
        rv = self.app.put('/blobs/222222f9dd5dc46ee476a8be155ab049994f7170',
                          data = {'size': 99,
                                  'git-sha1': '222222f9dd5dc46ee476a8be155ab03333333333',
                                  'content': 'foobar'})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/blobs/222222f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "222222f9dd5dc46ee476a8be155ab049994f7170"\n}'
