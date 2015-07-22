# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import json

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.backend import back
from swh.storage import db, models
from swh.storage import store

import test_initdb


def app_client(db_url="dbname=swhgitloader-test"):
    """Setup the application ready for testing.
    """
    back.app.config['conf'] = {'db_url': db_url,
                               'content_storage_dir':
                                   '/tmp/swh-git-loader/file-content-storage',
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
        assert rv.status_code == 200
        assert rv.data == b'Dev SWH API'

    @istest
    def get_404(self):
        # when
        rv = self.app.get('/nowhere')

        # then
        assert rv.status_code == 404

    @istest
    def get_bad_request(self):
        # when
        rv = self.app.get('/git/not-a-good-type/1')

        # then
        assert rv.status_code == 400
        assert rv.data == b'Bad request!'


@attr('slow')
class CommitTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        self.commit_sha1_hex = '000000f6dd5dc46ee476a8be155ab049994f717e'
        with db.connect(db_url) as db_conn:
            models.add_object(db_conn,
                              self.commit_sha1_hex,
                              models.Type.commit.value)

    @istest
    def get_commit_ok(self):
        # when
        rv = self.app.get('/git/commits/%s' % self.commit_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "000000f6dd5dc46ee476a8be155ab049994f717e"\n}'

    @istest
    def get_commit_not_found(self):
        # when
        rv = self.app.get('/git/commits/000000f6dd5dc46ee476a8be155ab049994f7170')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_commit_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/git/commits/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_commit_create_and_update(self):
        # does not exist
        rv = self.app.get('/git/commits/000000f6dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        rv = self.app.put('/git/commits/000000f6dd5dc46ee476a8be155ab049994f7170',
                          data={'content': 'commit-foo'})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/git/commits/000000f6dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "000000f6dd5dc46ee476a8be155ab049994f7170"\n}'

        # we update it
        rv = self.app.put('/git/commits/000000f6dd5dc46ee476a8be155ab049994f7170',
                          data={'content': 'commit-foo'})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/git/commits/000000f6dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "000000f6dd5dc46ee476a8be155ab049994f7170"\n}'


@attr('slow')
class TreeTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        self.tree_sha1_hex = '111111f9dd5dc46ee476a8be155ab049994f717e'
        with db.connect(db_url) as db_conn:
            models.add_object(db_conn, self.tree_sha1_hex,
                              models.Type.tree.value)

    @istest
    def get_tree_ok(self):
        # when
        rv = self.app.get('/git/trees/%s' % self.tree_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "111111f9dd5dc46ee476a8be155ab049994f717e"\n}'

    @istest
    def get_tree_not_found(self):
        # when
        rv = self.app.get('/git/trees/111111f9dd5dc46ee476a8be155ab049994f7170')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_tree_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/git/trees/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_tree_create_and_update(self):
        # does not exist
        rv = self.app.get('/git/trees/111111f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        rv = self.app.put('/git/trees/111111f9dd5dc46ee476a8be155ab049994f7170',
                          data={'content': 'tree-bar'})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/git/trees/111111f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "111111f9dd5dc46ee476a8be155ab049994f7170"\n}'

        # we update it
        rv = self.app.put('/git/trees/111111f9dd5dc46ee476a8be155ab049994f7170',
                          data={'content': 'tree-bar'})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/git/trees/111111f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "111111f9dd5dc46ee476a8be155ab049994f7170"\n}'


@attr('slow')
class BlobTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        self.blob_sha1_hex = '222222f9dd5dc46ee476a8be155ab049994f717e'
        blog_git_sha1 = '22222200000011111176a8be155ab049994f717e'
        with db.connect(db_url) as db_conn:
            models.add_blob(db_conn, self.blob_sha1_hex, 10, blog_git_sha1)

    @istest
    def get_blob_ok(self):
        # when
        rv = self.app.get('/git/blobs/%s' % self.blob_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "222222f9dd5dc46ee476a8be155ab049994f717e"\n}'

    @istest
    def get_blob_not_found(self):
        # when
        rv = self.app.get('/git/blobs/222222f9dd5dc46ee476a8be155ab049994f7170')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_blob_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/git/blobs/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    # NOTE:
    # As we store the sha1 as hexadecimal now. we no longer check this.
    # We assume this will be done by the db.
    #
    # @istest
    # def put_blob_bad_request_bad_payload(self):
    #     # when
    #     # we create it
    #     rv = self.app.put('/git/blobs/222222f9dd5dc46ee476a8be155ab049994f7170',
    #                       data = {'size': 99,
    #                               'git-sha1': 'bad-payload',
    #                               'content': 'foo'})

    #     # then
    #     assert rv.status_code == 400
    #     assert rv.data == b'Bad request!'

    @istest
    def put_blob_create_and_update(self):
        # does not exist
        rv = self.app.get('/git/blobs/222222f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        body = {'size': 99,
                'git-sha1': '222222f9dd5dc46ee476a8be155ab03333333333',
                'content': 'bar'}
        rv = self.app.put('/git/blobs/222222f9dd5dc46ee476a8be155ab049994f7170',
                          data=body)

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/git/blobs/222222f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "222222f9dd5dc46ee476a8be155ab049994f7170"\n}'

        # we update it
        body = {'size': 99,
                'git-sha1': '222222f9dd5dc46ee476a8be155ab03333333333',
                'content': 'foobar'}
        rv = self.app.put('/git/blobs/222222f9dd5dc46ee476a8be155ab049994f7170',
                          data=body)

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/git/blobs/222222f9dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "222222f9dd5dc46ee476a8be155ab049994f7170"\n}'


@attr('slow')
class TestObjectsCase(unittest.TestCase):
    def setUp(self):
        self.app, self.db_url = app_client()

        with db.connect(self.db_url) as db_conn:
            self.blob_sha1_hex = '000000111111c46ee476a8be155ab049994f717e'
            blog_git_sha1 = '00000011111122222276a8be155ab049994f717e'
            models.add_blob(db_conn, self.blob_sha1_hex, 10, blog_git_sha1)

            self.tree_sha1_hex = '111111f9dd5dc46ee476a8be155ab049994f717e'
            models.add_object(db_conn, self.tree_sha1_hex,
                              store.Type.tree.value)

            self.commit_sha1_hex = '222222f9dd5dc46ee476a8be155ab049994f717e'
            models.add_object(db_conn, self.commit_sha1_hex,
                              store.Type.commit.value)

        # check the insertion went ok!
        with db.connect(self.db_url) as db_conn:
            assert models.find_blob(db_conn, self.blob_sha1_hex) is not None
            assert models.find_object(db_conn, self.tree_sha1_hex,
                                      models.Type.tree.value) is not None
            assert models.find_object(db_conn, self.commit_sha1_hex,
                                      models.Type.commit.value) is not None

    @istest
    def get_non_presents_objects(self):
        # given

        # when
        payload = {'sha1s': [self.blob_sha1_hex,
                             self.tree_sha1_hex,
                             self.commit_sha1_hex,
                             self.commit_sha1_hex,
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '555444f9dd5dc46ee476a8be155ab049994f717e',
                             '666777f9dd5dc46ee476a8be155ab049994f717e']}
        json_payload = json.dumps(payload)

        rv = self.app.post('/objects/',
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
    def get_non_presents_objects_bad_requests(self):
        # given

        # when
        bad_payload = json.dumps({})

        rv = self.app.post('/objects/', data=bad_payload,
                           headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 400
        assert rv.data == b"Bad request! Expects 'sha1s' key with list of hexadecimal sha1s."

    @istest
    def put_non_presents_objects(self):
        # given
        payload_1 = {'sha1s': [self.blob_sha1_hex,
                               self.tree_sha1_hex,
                               self.commit_sha1_hex,
                               self.commit_sha1_hex,
                               '555444f9dd5dc46ee476a8be155ab049994f717e',
                               '555444f9dd5dc46ee476a8be155ab049994f717e',
                               '666777f9dd5dc46ee476a8be155ab049994f717e']}
        json_payload_1 = json.dumps(payload_1)

        rv = self.app.post('/objects/', data=json_payload_1,
                           headers={'Content-Type': 'application/json'})

        assert rv.status_code == 200

        json_result = json.loads(rv.data.decode('utf-8'))
        assert len(json_result.keys()) is 1                         # only 1 key
        sha1s = json_result['sha1s']
        assert len(sha1s) is 2                                      # only 2 sha1s
        assert "666777f9dd5dc46ee476a8be155ab049994f717e" in sha1s
        assert "555444f9dd5dc46ee476a8be155ab049994f717e" in sha1s

        # when
        payload_2 = {'555444f9dd5dc46ee476a8be155ab049994f717e':
                       {'sha1': '555444f9dd5dc46ee476a8be155ab049994f717e',
                        'size': 20,
                        'git-sha1': '555444f9dd5dc46ee476a8be155ab049994f717e',
                        'type': 'blob',
                        'content': 'blob\'s content'},
                     '555444f9dd5dc46ee476a8be155ab049994f717e':
                       {'sha1': '555444f9dd5dc46ee476a8be155ab049994f717e',
                        'content': 'tree content',
                        'type': 'tree'},
                     '666777f9dd5dc46ee476a8be155ab049994f717e':
                       {'sha1': '666777f9dd5dc46ee476a8be155ab049994f717e',
                        'type': 'commit',
                        'content': 'commit content'}}
        json_payload_2 = json.dumps(payload_2)

        rv = self.app.put('/objects/', data=json_payload_2,
                          headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 204

        rv = self.app.post('/objects/', data=json_payload_1,
                           headers={'Content-Type': 'application/json'})

        assert rv.status_code == 200

        json_result = json.loads(rv.data.decode('utf-8'))

        assert len(json_result.keys()) is 1
        assert len(json_result['sha1s']) is 0  # all sha1s are now knowns
