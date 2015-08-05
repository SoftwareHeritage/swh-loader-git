# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import json

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.storage import db, models

from test_utils import app_client


@attr('slow')
class OriginTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        with db.connect(db_url) as db_conn:
            self.origin_url = 'https://github.com/torvalds/linux.git'
            self.origin_type = 'git'
            models.add_origin(db_conn, self.origin_url, self.origin_type)

    @istest
    def get_origin_ok(self):
        # when
        payload = {'url': self.origin_url,
                   'type': self.origin_type}
        rv = self.app.post('/origins/',
                           data=json.dumps(payload),
                           headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 200
        json_result = json.loads(rv.data.decode('utf-8'))
        assert json_result['id']

    @istest
    def get_origin_not_found(self):
        # when
        payload = {'url': 'unknown',
                   'type': 'blah'}
        rv = self.app.post('/origins/',
                           data=json.dumps(payload),
                           headers={'Content-Type': 'application/json'})
        # then
        assert rv.status_code == 404
        assert rv.data == b'Origin not found!'

    @istest
    def get_origin_not_found_with_bad_format(self):
        payload = {'url': 'unknown'}  # url and type are mandatory
        rv = self.app.post('/origins/',
                           data=json.dumps(payload),
                           headers={'Content-Type': 'application/json'})
        # when
        rv = self.app.post('/origins/',
                           data=json.dumps(payload),
                           headers={'Content-Type': 'application/json'})
        # then
        assert rv.status_code == 400

    @istest
    def put_origin(self):
        # when
        payload = {'url': 'unknown',
                   'type': 'blah'}
        rv = self.app.post('/origins/',
                           data=json.dumps(payload),
                           headers={'Content-Type': 'application/json'})
        # then
        assert rv.status_code == 404
        assert rv.data == b'Origin not found!'

        # when
        rv = self.app.put('/origins/',
                          data=json.dumps(payload),
                          headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 200  # fixme 201
        json_result = json.loads(rv.data.decode('utf-8'))
        origin_id = json_result['id']
        assert origin_id

        payload = {'url': 'unknown',
                   'type': 'blah'}
        rv = self.app.post('/origins/',
                           data=json.dumps(payload),
                           headers={'Content-Type': 'application/json'})
        # then
        assert rv.status_code == 200
        json_result = json.loads(rv.data.decode('utf-8'))
        assert json_result['id'] == origin_id

        # when
        rv = self.app.put('/origins/',
                          data=json.dumps(payload),
                          headers={'Content-Type': 'application/json'})

        # then
        assert rv.status_code == 200  # fixme 204
        json_result = json.loads(rv.data.decode('utf-8'))
        assert json_result['id'] == origin_id
