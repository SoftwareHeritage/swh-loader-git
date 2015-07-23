# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import json
import time

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.backend import back
from swh.storage import db, models
from swh.storage import store

import test_initdb


def app_client(db_url="dbname=softwareheritage-dev-test"):
    """Setup the application ready for testing.
    """
    back.app.config['conf'] = {'db_url': db_url,
                               'content_storage_dir': '/tmp/swh-git-loader/content-storage',
                               'log_dir': '/tmp/swh-git-loader/log',
                               'folder_depth': 2,
                               'storage_compression': None,
                               'debug': 'true'}

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
        rv = self.app.get('/vcs/not-a-good-type/1')

        # then
        assert rv.status_code == 400
        assert rv.data == b'Bad request!'


@attr('slow')
class ContentTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        self.content_sha1_id = '222222f9dd5dc46ee476a8be155ab049994f717e'
        content_sha1_id = 'blabliblablo'
        self.content_sha256_hex = '222222f9dd5dc46ee476a8be155ab049994f717e'
        with db.connect(db_url) as db_conn:
            models.add_content(db_conn,
                               self.content_sha1_id,
                               content_sha1_id,
                               self.content_sha256_hex,
                               10)

    @istest
    def get_content_ok(self):
        # when
        rv = self.app.get('/vcs/contents/%s' % self.content_sha1_id)

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "222222f9dd5dc46ee476a8be155ab049994f717e"\n}'

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
        content_sha1 = 'sha1-contentc46ee476a8be155ab03333333333'

        # does not exist
        rv = self.app.get('/vcs/contents/%s' % content_sha1)

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        body = {'sha1': content_sha1,
                'content-sha1': 'content-sha1c46ee476a8be155ab03333333333',
                'content-sha256': 'content-sha2566ee476a8be155ab03333333333',
                'content': 'bar',
                'size': '3'}

        rv = self.app.put('/vcs/contents/%s' % content_sha1,
                          data=json.dumps(body),
                          headers={'Content-Type': 'application/json'})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/contents/%s' % content_sha1)

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "sha1-contentc46ee476a8be155ab03333333333"\n}'

        # # we update it
        body = {'sha1': content_sha1,
                'content-sha1': 'content-sha1c46ee476a8be155ab03333333333',
                'content-sha256': 'content-sha2566ee476a8be155ab03333333333',
                'content': 'bar',
                'size': '3'}

        rv = self.app.put('/vcs/contents/%s' % content_sha1,
                          data=body)

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/vcs/contents/%s' % content_sha1)

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "sha1-contentc46ee476a8be155ab03333333333"\n}'


@attr('slow')
class DirectoryTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        self.content_sha1_id = 'content-sha1c46ee476a8be155ab049994f717e'
        content_sha1_hex = 'content-sha1c46ee476a8be155ab049994f717e'
        content_sha256_hex = 'content-sha2566ee476a8be155ab049994f717e'
        with db.connect(db_url) as db_conn:
            models.add_content(db_conn,
                               self.content_sha1_id,
                               content_sha1_hex,
                               content_sha256_hex,
                               10)

        self.directory_sha1_hex = 'directory-sha16ee476a8be155ab049994f717e'
        with db.connect(db_url) as db_conn:
            models.add_directory(db_conn, self.directory_sha1_hex)

    @istest
    def get_directory_ok(self):
        # when
        rv = self.app.get('/vcs/directories/%s' % self.directory_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "directory-sha16ee476a8be155ab049994f717e"\n}'

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

        date_str = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())

        # we create it
        body = json.dumps({'content': 'directory has content too.',
                           'entries': [{'name': 'filename',
                                        'target-sha1': self.content_sha1_id,
                                        'nature': 'file',
                                        'perms': '000',
                                        'atime': date_str,
                                        'mtime': date_str,
                                        'ctime': date_str,
                                        'parent': directory_sha1},
                                       {'name': 'dirname',
                                        'target-sha1': self.directory_sha1_hex,
                                        'nature': 'directory',
                                        'perms': '012',
                                        'atime': date_str,
                                        'mtime': date_str,
                                        'ctime': date_str,
                                        'parent': directory_sha1}
                                      ]})

        rv = self.app.put('/vcs/directories/%s' % directory_sha1,
                           data=body,
                           headers={'Content-Type': 'application/json'})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/directories/%s' % directory_sha1)

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "directory-sha16ee476a8be155ab049994f7170"\n}'

        # we update it
        rv = self.app.put('/vcs/directories/directory-sha16ee476a8be155ab049994f7170',
                          data={'entry': 'directory-bar'})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/vcs/directories/directory-sha16ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "directory-sha16ee476a8be155ab049994f7170"\n}'


# @attr('slow')
# class RevisionTestCase(unittest.TestCase):
#     def setUp(self):
#         self.app, db_url = app_client()

#         self.revision_sha1_hex = '000000f6dd5dc46ee476a8be155ab049994f717e'
#         with db.connect(db_url) as db_conn:
#             models.add_object(db_conn,
#                               self.revision_sha1_hex,
#                               models.Type.revision.value)

#     @istest
#     def get_revision_ok(self):
#         # when
#         rv = self.app.get('/vcs/revisions/%s' % self.revision_sha1_hex)

#         # then
#         assert rv.status_code == 200
#         assert rv.data == b'{\n  "sha1": "000000f6dd5dc46ee476a8be155ab049994f717e"\n}'

#     @istest
#     def get_revision_not_found(self):
#         # when
#         rv = self.app.get('/vcs/revisions/000000f6dd5dc46ee476a8be155ab049994f7170')
#         # then
#         assert rv.status_code == 404
#         assert rv.data == b'Not found!'

#     @istest
#     def get_revision_not_found_with_bad_format(self):
#         # when
#         rv = self.app.get('/vcs/revisions/1')
#         # then
#         assert rv.status_code == 404
#         assert rv.data == b'Not found!'

#     @istest
#     def put_revision_create_and_update(self):
#         # does not exist
#         rv = self.app.get('/vcs/revisions/000000f6dd5dc46ee476a8be155ab049994f7170')

#         # then
#         assert rv.status_code == 404
#         assert rv.data == b'Not found!'

#         # we create it
#         rv = self.app.put('/vcs/revisions/000000f6dd5dc46ee476a8be155ab049994f7170',
#                           data={'content': 'revision-foo'})

#         assert rv.status_code == 204
#         assert rv.data == b''

#         # now it exists
#         rv = self.app.get('/vcs/revisions/000000f6dd5dc46ee476a8be155ab049994f7170')

#         # then
#         assert rv.status_code == 200
#         assert rv.data == b'{\n  "sha1": "000000f6dd5dc46ee476a8be155ab049994f7170"\n}'

#         # we update it
#         rv = self.app.put('/vcs/revisions/000000f6dd5dc46ee476a8be155ab049994f7170',
#                           data={'content': 'revision-foo'})

#         assert rv.status_code == 200
#         assert rv.data == b'Successful update!'

#         # still the same
#         rv = self.app.get('/vcs/revisions/000000f6dd5dc46ee476a8be155ab049994f7170')

#         # then
#         assert rv.status_code == 200
#         assert rv.data == b'{\n  "sha1": "000000f6dd5dc46ee476a8be155ab049994f7170"\n}'




# @attr('slow')
# class TestObjectsCase(unittest.TestCase):
#     def setUp(self):
#         self.app, self.db_url = app_client()

#         with db.connect(self.db_url) as db_conn:
#             self.content_sha1_id = '000000111111c46ee476a8be155ab049994f717e'
#             blog_git_sha1 = '00000011111122222276a8be155ab049994f717e'
#             models.add_content(db_conn, self.content_sha1_id, 10, blog_git_sha1)

#             self.directory_sha1_hex = '111111f9dd5dc46ee476a8be155ab049994f717e'
#             models.add_object(db_conn, self.directory_sha1_hex,
#                               store.Type.directory.value)

#             self.revision_sha1_hex = '222222f9dd5dc46ee476a8be155ab049994f717e'
#             models.add_object(db_conn, self.revision_sha1_hex,
#                               store.Type.revision.value)

#         # check the insertion went ok!
#         with db.connect(self.db_url) as db_conn:
#             assert models.find_content(db_conn, self.content_sha1_id) is not None
#             assert models.find_object(db_conn, self.directory_sha1_hex,
#                                       models.Type.directory.value) is not None
#             assert models.find_object(db_conn, self.revision_sha1_hex,
#                                       models.Type.revision.value) is not None

#     @istest
#     def get_non_presents_objects(self):
#         # given

#         # when
#         payload = {'sha1s': [self.content_sha1_id,
#                              self.directory_sha1_hex,
#                              self.revision_sha1_hex,
#                              self.revision_sha1_hex,
#                              '555444f9dd5dc46ee476a8be155ab049994f717e',
#                              '555444f9dd5dc46ee476a8be155ab049994f717e',
#                              '666777f9dd5dc46ee476a8be155ab049994f717e']}
#         json_payload = json.dumps(payload)

#         rv = self.app.post('/objects/',
#                            data=json_payload,
#                            headers={'Content-Type': 'application/json'})

#         # then
#         assert rv.status_code == 200

#         json_result = json.loads(rv.data.decode('utf-8'))
#         assert len(json_result.keys()) is 1                                       # only 1 key
#         assert len(json_result['sha1s']) is 2                                     # only 2 sha1s
#         sha1s = json_result['sha1s']
#         assert "666777f9dd5dc46ee476a8be155ab049994f717e" in sha1s
#         assert "555444f9dd5dc46ee476a8be155ab049994f717e" in sha1s

#     @istest
#     def get_non_presents_objects_bad_requests(self):
#         # given

#         # when
#         bad_payload = json.dumps({})

#         rv = self.app.post('/objects/', data=bad_payload,
#                            headers={'Content-Type': 'application/json'})

#         # then
#         assert rv.status_code == 400
#         assert rv.data == b"Bad request! Expects 'sha1s' key with list of hexadecimal sha1s."

#     @istest
#     def put_non_presents_objects(self):
#         # given
#         payload_1 = {'sha1s': [self.content_sha1_id,
#                                self.directory_sha1_hex,
#                                self.revision_sha1_hex,
#                                self.revision_sha1_hex,
#                                '555444f9dd5dc46ee476a8be155ab049994f717e',
#                                '555444f9dd5dc46ee476a8be155ab049994f717e',
#                                '666777f9dd5dc46ee476a8be155ab049994f717e']}
#         json_payload_1 = json.dumps(payload_1)

#         rv = self.app.post('/objects/', data=json_payload_1,
#                            headers={'Content-Type': 'application/json'})

#         assert rv.status_code == 200

#         json_result = json.loads(rv.data.decode('utf-8'))
#         assert len(json_result.keys()) is 1                         # only 1 key
#         sha1s = json_result['sha1s']
#         assert len(sha1s) is 2                                      # only 2 sha1s
#         assert "666777f9dd5dc46ee476a8be155ab049994f717e" in sha1s
#         assert "555444f9dd5dc46ee476a8be155ab049994f717e" in sha1s

#         # when
#         payload_2 = {'555444f9dd5dc46ee476a8be155ab049994f717e':
#                        {'sha1': '555444f9dd5dc46ee476a8be155ab049994f717e',
#                         'size': 20,
#                         'git-sha1': '555444f9dd5dc46ee476a8be155ab049994f717e',
#                         'type': 'content',
#                         'content': 'content\'s content'},
#                      '555444f9dd5dc46ee476a8be155ab049994f717e':
#                        {'sha1': '555444f9dd5dc46ee476a8be155ab049994f717e',
#                         'content': 'directory content',
#                         'type': 'directory'},
#                      '666777f9dd5dc46ee476a8be155ab049994f717e':
#                        {'sha1': '666777f9dd5dc46ee476a8be155ab049994f717e',
#                         'type': 'revision',
#                         'content': 'revision content'}}
#         json_payload_2 = json.dumps(payload_2)

#         rv = self.app.put('/objects/', data=json_payload_2,
#                           headers={'Content-Type': 'application/json'})

#         # then
#         assert rv.status_code == 204

#         rv = self.app.post('/objects/', data=json_payload_1,
#                            headers={'Content-Type': 'application/json'})

#         assert rv.status_code == 200

#         json_result = json.loads(rv.data.decode('utf-8'))

#         assert len(json_result.keys()) is 1
#         assert len(json_result['sha1s']) is 0  # all sha1s are now knowns
