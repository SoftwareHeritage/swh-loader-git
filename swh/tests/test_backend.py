# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import json
import time

from datetime import datetime

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.backend import back
from swh.storage import db, models
from swh.storage import store

import test_initdb

def now():
    "Build the date as of now in the api's format."
    return time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())


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

        with db.connect(db_url) as db_conn:
            self.content_sha1_id = '222222f9dd5dc46ee476a8be155ab049994f717e'
            content_sha1_id = 'blabliblablo'
            self.content_sha256_hex = '222222f9dd5dc46ee476a8be155ab049994f717e'
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

        # we create it
        body = json.dumps({'content': 'directory has content too.',
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
        assert rv.data.decode('utf-8') == '{\n  "sha1": "%s"\n}' % self.revision_sha1_hex

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
        body = json.dumps({'content': 'revision has content too.',
                           'date': now(),
                           'directory': self.directory_sha1_hex,
                           'message': 'revision message describing it',
                           'committer': 'ardumont',
                           'author': 'ardumont'})

        rv = self.app.put('/vcs/revisions/%s' % revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': 'application/json'})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/revisions/%s' % revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data.decode('utf-8') == '{\n  "sha1": "%s"\n}' % revision_sha1_hex

        # we update it
        rv = self.app.put('/vcs/revisions/%s' % revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': 'application/json'})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/vcs/revisions/%s' % revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data.decode('utf-8') == '{\n  "sha1": "%s"\n}' % revision_sha1_hex


@attr('slow')
class ReleaseTestCase(unittest.TestCase):
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

            self.release_sha1_hex = 'release-sha1-to-test-existence1234567901'
            models.add_release(db_conn,
                               self.release_sha1_hex,
                               self.revision_sha1_hex,
                               now(),
                               "0.0.1",
                               "Super release tagged by tony")

    @istest
    def get_release_ok(self):
        # when
        rv = self.app.get('/vcs/releases/%s' % self.release_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data.decode('utf-8') == '{\n  "sha1": "%s"\n}' % self.release_sha1_hex

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
        body = json.dumps({'sha1': release_sha1_hex,
                           'content': 'release also has content',
                           'revision': self.revision_sha1_hex,
                           'date': now(),
                           'name': '0.0.1',
                           'comment': 'super release tagged by ardumont'})

        rv = self.app.put('/vcs/releases/%s' % release_sha1_hex,
                          data=body,
                          headers={'Content-Type': 'application/json'})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/releases/%s' % release_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data.decode('utf-8') == '{\n  "sha1": "%s"\n}' % release_sha1_hex

        # we update it
        rv = self.app.put('/vcs/releases/%s' % release_sha1_hex,
                          data=body,
                          headers={'Content-Type': 'application/json'})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/vcs/releases/%s' % release_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data.decode('utf-8') == '{\n  "sha1": "%s"\n}' % release_sha1_hex


@attr('slow')
class OccurrenceTestCase(unittest.TestCase):
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

            self.origin_url = "https://github.com/user/repo"
            models.add_origin(db_conn, "git", self.origin_url)

            models.add_occurrence(db_conn,
                                 self.origin_url,
                                 'master',
                                 self.revision_sha1_hex)

            self.revision_sha1_hex_2 = '2-revision-sha1-to-test-existence9994f71'
            models.add_revision(db_conn,
                                self.revision_sha1_hex_2,
                                now(),
                                self.directory_sha1_hex,
                                "revision message 2",
                                "ardumont",
                                "ardumont")


    @istest
    def get_occurrence_ok(self):
        # when
        rv = self.app.get('/vcs/occurrences/%s' % self.revision_sha1_hex)

        # then
        assert rv.status_code == 200
        # assert rv.data.decode('utf-8') == '{\n  "names": ["master"]\n}'  # return the list of references pointing to the revision -> do not care for the moment

    @istest
    def get_occurrence_not_found(self):
        # when
        rv = self.app.get('/vcs/occurrences/inexistant-sha1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_occurrence_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/vcs/occurrences/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_occurrence_create_and_update(self):
        occ_revision_sha1_hex = self.revision_sha1_hex_2

        rv = self.app.get('/vcs/occurrences/%s' % occ_revision_sha1_hex)

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        body = json.dumps({'content': 'occurrence content',
                           'reference': 'master',
                           'url-origin': self.origin_url})

        rv = self.app.put('/vcs/occurrences/%s' % occ_revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': 'application/json'})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/occurrences/%s' % occ_revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data.decode('utf-8') == '{\n  "sha1": "%s"\n}' % occ_revision_sha1_hex

        # we update it
        rv = self.app.put('/vcs/occurrences/%s' % occ_revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': 'application/json'})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/vcs/occurrences/%s' % occ_revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert rv.data.decode('utf-8') == '{\n  "sha1": "%s"\n}' % occ_revision_sha1_hex


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

            self.release_sha1_hex = 'release-sha1-to-test-existence1234567901'
            models.add_release(db_conn,
                               self.release_sha1_hex,
                               self.revision_sha1_hex,
                               now(),
                               "0.0.1",
                               "Super release tagged by tony")

    @istest
    def get_non_presents_objects(self):
        # given

        # when
        payload = {'sha1s': [self.content_sha1_id,
                             self.directory_sha1_hex,
                             self.revision_sha1_hex,
                             self.revision_sha1_hex,
                             self.release_sha1_hex,
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
        content_sha1_unknown1 = 'content-sha1-46ee476a8be155ab049994f717e'
        content_sha1_unknown2 = 'content-sha1-2-ee476a8be155ab049994f717e'
        directory_sha1_unknown = 'directory-sha1-46ee476a8be155ab049994f717e'
        release_sha1_unknown = 'release-sha1-46ee476a8be155ab049994f717e'

        # given
        payload_1 = {'sha1s': [self.content_sha1_id,
                               self.directory_sha1_hex,
                               self.revision_sha1_hex,
                               self.revision_sha1_hex,
                               self.release_sha1_hex,
                               content_sha1_unknown1,
                               content_sha1_unknown1,  # duplicates is not a concern
                               content_sha1_unknown2,
                               directory_sha1_unknown,
                               release_sha1_unknown]}

        json_payload_1 = json.dumps(payload_1)

        rv = self.app.post('/objects/', data=json_payload_1,
                           headers={'Content-Type': 'application/json'})

        assert rv.status_code == 200

        json_result = json.loads(rv.data.decode('utf-8'))
        assert len(json_result.keys()) is 1                         # only 1 key
        sha1s = json_result['sha1s']
        assert len(sha1s) is 4                                      # only 4 sha1s
        assert content_sha1_unknown1 in sha1s
        assert content_sha1_unknown2 in sha1s
        assert directory_sha1_unknown in sha1s
        assert release_sha1_unknown in sha1s

        # when
        payload_contents = [{'sha1': content_sha1_unknown1,
                             'content-sha1': 'content-sha1c46ee476a8be155ab03333333333',
                             'content-sha256': 'content-sha2566ee476a8be155ab03333333333',
                             'content': 'bar',
                             'size': '3'},
                            {'sha1': content_sha1_unknown2,
                             'content-sha1': '555444f9dd5dc46ee476a8be155ab049994f717e',
                             'content-sha256': '555444f9dd5dc46ee476a8be155ab049994f717e',
                             'content': 'foobar',
                             'size': 6}]
        json_payload_contents = json.dumps(payload_contents)

        rv = self.app.put('/vcs/contents/',
                          data=json_payload_contents,
                          headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 204

        # Sent back the first requests and see that we now have less unknown
        # sha1s (no more missed contents )
        rv = self.app.post('/objects/', data=json_payload_1,
                           headers={'Content-Type': 'application/json'})

        assert rv.status_code == 200

        json_result = json.loads(rv.data.decode('utf-8'))
        assert len(json_result.keys()) is 1                         # only 1 key
        sha1s = json_result['sha1s']
        assert len(sha1s) is 2                                      # only 2 sha1s
        assert directory_sha1_unknown in sha1s
        assert release_sha1_unknown in sha1s

        # when
        payload_directories = [{'sha1': directory_sha1_unknown,
                                'content': 'directory has content too.',
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

        json_payload_directories = json.dumps(payload_directories)

        rv = self.app.put('/vcs/directories/',
                          data=json_payload_directories,
                          headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 204

        # Sent back the first requests and see that we now have less unknown
        # sha1s (no more missed directories)
        rv = self.app.post('/objects/',
                           data=json_payload_1,
                           headers={'Content-Type': 'application/json'})

        assert rv.status_code == 200

        json_result = json.loads(rv.data.decode('utf-8'))
        assert len(json_result.keys()) is 1                         # only 1 key
        sha1s = json_result['sha1s']
        assert len(sha1s) is 1                                      # only 1 sha1 unknown
        assert release_sha1_unknown in sha1s

        # when
        payload_releases = [{'sha1': release_sha1_unknown,
                             'content': 'release also has content',
                             'revision': self.revision_sha1_hex,
                             'date': now(),
                             'name': '0.0.1',
                             'comment': 'super release tagged by ardumont'},
                            {'sha1': 'another-sha1',
                             'content': 'some content',
                             'revision': self.revision_sha1_hex,
                             'date': now(),
                             'name': '0.0.2',
                             'comment': 'fix bugs release by zack and olasd'}]

        json_payload_releases = json.dumps(payload_releases)

        rv = self.app.put('/vcs/releases/',
                          data=json_payload_releases,
                          headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 204

        # Sent back the first requests and see that we now have less unknown
        # sha1s (no more missed directories)
        rv = self.app.post('/objects/',
                           data=json_payload_1,
                           headers={'Content-Type': 'application/json'})

        assert rv.status_code == 200

        json_result = json.loads(rv.data.decode('utf-8'))
        assert len(json_result.keys()) is 1                         # only 1 key
        sha1s = json_result['sha1s']
        assert len(sha1s) is 0
