# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.backend import back
from swh import db, hash
from swh.gitloader import models

import test_initdb


def app_client(db_url="dbname=swhgitloader-test"):
    """Setup the application ready for testing.
    """
    back.app.config['conf'] = {'db_url': db_url}
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
        # given
        rv = self.app.get('/')

        # then
        assert rv.status_code is 200
        assert rv.data == b'Dev SWH API'


@attr('slow')
class CommitTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        self.commit_sha1_hex = '62745df6dd5dc46ee476a8be155ab049994f717e'
        self.commit_sha1_bin = hash.sha1_bin(self.commit_sha1_hex)
        with db.connect(db_url) as db_conn:
            models.add_object(db_conn, self.commit_sha1_bin, models.Type.commit)

    @istest
    def get_commit_ok(self):
        # given
        rv = self.app.get('/commits/%s' % self.commit_sha1_hex)

        # then
        assert rv.status_code is 200
        assert rv.data == b'{\n  "sha1": "62745df6dd5dc46ee476a8be155ab049994f717e"\n}'

    @istest
    def get_commit_not_found(self):
        # given
        rv = self.app.get('/commits/62745df6dd5dc46ee476a8be155ab049994f7170')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_commit_bad_request(self):
        # given
        rv = self.app.get('/commits/1')
        # then
        assert rv.status_code == 400
        assert rv.data == b'Bad request!'

    @istest
    def put_commit_create_and_update(self):
        # does not exist
        rv = self.app.get('/commits/62745df6dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        rv = self.app.put('/commits/62745df6dd5dc46ee476a8be155ab049994f7170')

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/commits/62745df6dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "62745df6dd5dc46ee476a8be155ab049994f7170"\n}'

        # we update it
        rv = self.app.put('/commits/62745df6dd5dc46ee476a8be155ab049994f7170')

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/commits/62745df6dd5dc46ee476a8be155ab049994f7170')

        # then
        assert rv.status_code == 200
        assert rv.data == b'{\n  "sha1": "62745df6dd5dc46ee476a8be155ab049994f7170"\n}'
