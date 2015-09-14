# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.loader.git.storage import db, models
from swh.loader.git.protocols import serial
from test_utils import app_client


@attr('slow')
class OriginTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app, db_url, _ = app_client()

        with db.connect(db_url) as db_conn:
            self.origin_url = 'https://github.com/torvalds/linux.git'
            self.origin_type = 'git'
            self.origin_id = models.add_origin(db_conn, self.origin_url, self.origin_type)

    @istest
    def get_origin_ok(self):
        # when
        payload = {'url': self.origin_url,
                   'type': self.origin_type}
        rv = self.app.post('/vcs/origins/',
                           data=serial.dumps(payload),
                           headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == self.origin_id

    @istest
    def get_origin_not_found(self):
        # when
        payload = {'url': 'unknown',
                   'type': 'blah'}
        rv = self.app.post('/vcs/origins/',
                           data=serial.dumps(payload),
                           headers={'Content-Type': serial.MIMETYPE})
        # then
        assert rv.status_code == 404
        assert rv.data == b'Origin not found!'

    @istest
    def get_origin_not_found_with_bad_format(self):
        # when
        rv = self.app.post('/vcs/origins/',
                           data=serial.dumps({'url': 'unknown'}),
                           headers={'Content-Type': serial.MIMETYPE})
        # then
        assert rv.status_code == 400

    @istest
    def put_origin(self):
        # when
        payload = {'url': 'unknown',
                   'type': 'blah'}
        rv = self.app.post('/vcs/origins/',
                           data=serial.dumps(payload),
                           headers={'Content-Type': serial.MIMETYPE})
        # then
        assert rv.status_code == 404
        assert rv.data == b'Origin not found!'

        # when
        rv = self.app.put('/vcs/origins/',
                          data=serial.dumps(payload),
                          headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 200  # FIXME: 201
        assert serial.loads(rv.data)['id']

        payload = {'url': 'unknown',
                   'type': 'blah'}
        rv = self.app.post('/vcs/origins/',
                           data=serial.dumps(payload),
                           headers={'Content-Type': serial.MIMETYPE})
        # then
        assert rv.status_code == 200
        origin_id = serial.loads(rv.data)['id']
        assert origin_id

        # when
        rv = self.app.put('/vcs/origins/',
                          data=serial.dumps(payload),
                          headers={'Content-Type': serial.MIMETYPE})

        # then
        assert rv.status_code == 200  # FIXME: 204
        assert serial.loads(rv.data)['id'] == origin_id
